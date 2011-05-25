require 'rubygems'
require 'bud'
require 'chord/chord_find'

module ChordStabilize
  import ChordFind => :new_succ_finder
  # import ChordFind => :succ_pred_finder
  import ChordFind => :fix_finger_finder

  state do
    periodic :fix_fingers, 2
    periodic :stable_timer, 4
    interface input, :join_up, [:to, :start]
    channel :proxy_succ, [:@to, :succ, :succ_addr]
    channel :join_req, [:@to, :requestor_addr, :start]
    table   :join_pending, join_req.schema
    channel :succ_pred_req, [:@to, :from]
    channel :succ_pred_resp, [:@to, :from, :pred_id, :pred_addr]
    channel :succ_notify, [:@to, :start, :addr]
    channel :xfer_keys_ack, [:@ackee, :keyval, :acker, :ack_start]
    channel :xfer_keys, [:@receiver, :keyval, :sender]
    table :offsets, [:val]
    scratch :rands, [:val]
    scratch :rand_ix, [] => [:val]
  end

  bootstrap do
    offsets <= (1..log2(@maxkey)).map{|o| [o]}
  end
  
  bloom :simple_join do
    # send out the join request
    join_req <~ join_up {|j| [j.to, ip_port, j.start]}

    # handle the join request by finding successor
    new_succ_finder.succ_req <= join_req do |j| 
      # puts "at #{ip_port}, received join_req from #{j.requestor_addr}"
      [j.start]
    end
    join_pending <= join_req
    
    # respond to the join request
    proxy_succ <~ (new_succ_finder.succ_resp * join_pending).pairs(:key => :start) do |s,j| 
      # puts "at #{ip_port}, found successor for #{j.requestor_addr}: #{s.key}"
      [j.requestor_addr, s.start, s.addr] 
    end
    # delete pending tuple
    join_pending <- (new_succ_finder.succ_resp * join_pending).rights(:key => :start)
    
    # when we get proxy_succ response, add successor finger
    finger <+ proxy_succ do |p|
      # puts "proxy_succ received, adding finger #{[0, (me.first.start + 1) % @maxkey, (me.first.start + 2) % @maxkey, p.succ, p.succ_addr].inspect}"
      [0, (me.first.start + 1) % @maxkey, (me.first.start + 2) % @maxkey, p.succ, p.succ_addr]
    end    
    
    # and initialize non-successors with nils
    finger <+ (proxy_succ * offsets).combos do |p,o|
      [o.val-1, (me.first.start + 2**(o.val-1)) % @maxkey, (me.first.start + 2**o.val) % @maxkey, nil, nil] unless o.val == 1
    end
  end

  bloom :stabilize do
    succ_pred_req <~ stable_timer do |s| 
      unless finger[[0]].nil? or finger[[0]].succ_addr.nil?
        [finger[[0]].succ_addr, ip_port] 
      end
    end
    succ_pred_resp <~ succ_pred_req { |s| [s.from, ip_port, me.first.pred_id, me.first.pred_addr] }
    finger <+ succ_pred_resp do |s|
      if in_range(s.pred_id, me.first.start, finger[[0]].succ)
        # puts "at #{ip_port}, updating successor to #{s.pred_id}(#{s.pred_addr})"
        [0, (me.first.start + 1) % @maxkey, (me.first.start + 2) % @maxkey, s.pred_id, s.pred_addr]
      end
    end
    finger <- succ_pred_resp do |s|
      if in_range(s.pred_id, me.first.start, finger[[0]].succ)
        finger[[0]]
      end      
    end
    succ_notify <~ succ_pred_resp do |s| 
      if in_range(s.pred_id, me.first.start, finger[[0]].succ)
        dest = s.pred_addr
      else
        dest = finger[[0]].succ_addr
      end
      # puts "at #{ip_port}, notifying successor: #{[dest, me.first.start, ip_port].inspect}"
      [dest, me.first.start, ip_port]
    end
  end

  bloom :notify do
    me <+ succ_notify do |s| 
      if me.first.pred_id.nil? or in_range(s.start, me.first.pred_id, me.first.start)
        # puts "at #{ip_port} updating me for new pred: #{[me.first.start, s.start, s.addr].inspect}"
        [me.first.start, s.start, s.addr] 
      end
    end
    me <- succ_notify do |s| 
      if me.first.pred_id.nil? or in_range(s.start, me.first.pred_id, me.first.start) 
        # puts "at #{ip_port} removing me val: #{me.first.inspect}"
        me.first 
      end
    end
    
    # initiate key transfer for my keys that *precede* my new predecessor
    xfer_keys <~ (succ_notify * localkeys).combos do |s,l|
      [s.addr, l, ip_port] if in_range(l.key, me.first.start, s.start, false, true)
    end

    # when we receive keys being transfered, put them in localkeys
    localkeys <= xfer_keys {|x| x.keyval }
    # stdio <~ xfer_keys {|x| ["got key xfer #{x.inspect}"]}
    # and ack
    xfer_keys_ack <~ xfer_keys {|x| [x.sender, x.keyval, ip_port, me.first.start]}    

    # once key transfer succeeds, delete the transfered keys
    localkeys <- xfer_keys_ack do |x|
      # puts "deleting #{x.keyval.inspect}" if in_range(x.keyval[0], x.ack_start, me.first.start, true, false)
      x.keyval if in_range(x.keyval[0], me.first.start, x.ack_start, false, true)
    end        
    
  end

  bloom :fix_dem_fingers do
    # fix nil fingers
    fix_finger_finder.succ_req <= (fix_fingers * finger).pairs do |fix, fing|
      [fing.start] if fing.succ.nil?
    end
    
    # fix a random finger
    rands <= [[rand(log2(@maxkey))]]
    rand_ix <= rands.argagg(:choose_rand, [], :val)
    fix_finger_finder.succ_req <= fix_fingers do |f|
      # puts "rand_ix is empty, rands length is #{rands.length}" if rand_ix.length == 0
      unless rand_ix.length == 0 or finger[ [rand_ix.first.val] ].nil? or finger[ [rand_ix.first.val] ].start.nil? or finger[ [rand_ix.first.val] ].succ.nil?
        [finger[ [rand_ix.first.val] ].start]        
      end
    end
    # stdio <~ fix_finger_finder.succ_req { |s| ["searching for finger[[#{s.key}]] (#{finger[[s.key]].start})@#{ip_port}"] if finger[[s.key]] and ip_port == '127.0.0.1:12346'}
    finger <+ (fix_finger_finder.succ_resp * finger).pairs(:key => :start) do |s,f|
      # puts "fixing finger to #{[f.index, f.start, f.hi, s.start, s.addr].inspect} at #{ip_port}" if ip_port = '127.0.0.1:12346' and not s.start == f.succ
      [f.index, f.start, f.hi, s.start, s.addr] unless s.start == f.succ
    end
    finger <- (fix_finger_finder.succ_resp * finger).pairs(:key => :start) do |s,f|
      f unless s.start == f.succ
    end
  end
end