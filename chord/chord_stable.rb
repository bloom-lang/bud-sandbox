require 'rubygems'
require 'bud'
require 'chord/chord_find'

# utility module to get the predecessor of one's successor
module ChordSuccPred
  state do
    interface :input, :succ_pred_req, [:to, :from, :hops]
    interface :output, :succ_pred_resp, [:@to, :from, :pred_id, :pred_addr, :hop_num]
    channel :sp_req, [:@to, :from, :hops]
    channel :sp_resp, [:@to, :from, :pred_id, :pred_addr, :hop_num]
  end
  
  bloom do
    # respond to request
    sp_resp <~ sp_req { |s| [s.from, ip_port, me.first.pred_id, me.first.pred_addr, s.hops] }
    # and forward along if hops > 1
    sp_req  <~ sp_req do |s| 
      [finger[[0]].succ_addr, s.from, s.hops - 1] unless finger[[0]].nil? or s.hops <= 1
    end
    
    # map interfaces to channels
    sp_req <~ succ_pred_req
    succ_pred_resp <= sp_resp
  end
end

# Stabilization protocol from the Chord paper.
module ChordStabilize
  import ChordFind => :new_succ_finder
  import ChordFind => :fix_finger_finder
  import ChordSuccPred => :sp

  state do
    periodic :fix_fingers, 2
    periodic :stable_timer, 2
    interface input, :join_up, [:to, :start]
    channel :proxy_succ, [:@to, :succ, :succ_addr]
    channel :join_req, [:@to, :requestor_addr, :start]
    table   :join_pending, join_req.schema
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
  
  # simple logic for join -- use proxy to connect new node to its successor
  bloom :simple_join do
    # to begin, new node sends join request to the proxy
    join_req <~ join_up {|j| [j.to, ip_port, j.start]}

    # the proxy handles the join request by asynchronously finding new node's successor
    new_succ_finder.succ_req <= join_req do |j| 
      # puts "at #{ip_port}, received join_req from #{j.requestor_addr}"
      [j.start]
    end
    join_pending <= join_req
    # upon response to the async find, respond to the new node's join request
    proxy_succ <~ (new_succ_finder.succ_resp * join_pending).pairs(:key => :start) do |s,j| 
      [j.requestor_addr, s.start, s.addr] 
    end
    # delete pending tuple
    join_pending <- (new_succ_finder.succ_resp * join_pending).rights(:key => :start)
    
    # at the new node, when we get proxy_succ response, add successor (i.e. finger[[0]])
    finger <+ proxy_succ do |p|
      [0, (me.first.start + 1) % @maxkey, (me.first.start + 2) % @maxkey, p.succ, p.succ_addr]
    end        
    # and initialize non-successor fingers with nils
    finger <+ (proxy_succ * offsets).combos do |p,o|
      [o.val-1, (me.first.start + 2**(o.val-1)) % @maxkey, (me.first.start + 2**o.val) % @maxkey, nil, nil] unless o.val == 1
    end
  end

  # periodically "stabilize": make sure each node n's successor's predecessor is n
  bloom :stabilize do
    # request successor's predecessor information
    sp.succ_pred_req <= stable_timer do |s| 
      unless finger[[0]].nil? or finger[[0]].succ_addr.nil?
        [finger[[0]].succ_addr, ip_port, 1] 
      end
    end
        
    # upon receiving response, if it is between me and finger[0], update finger[0] to successors predecessor
    finger <+ sp.succ_pred_resp do |s|
      if in_range(s.pred_id, me.first.start, finger[[0]].succ)
        # puts "at #{ip_port}, updating successor to #{s.pred_id}(#{s.pred_addr})"
        [0, (me.first.start + 1) % @maxkey, (me.first.start + 2) % @maxkey, s.pred_id, s.pred_addr]
      end
    end
    finger <- sp.succ_pred_resp do |s|
      if in_range(s.pred_id, me.first.start, finger[[0]].succ)
        finger[[0]]
      end      
    end
    # each round of stabilization update proper successor node to point its predecessor here
    succ_notify <~ sp.succ_pred_resp do |s| 
      if in_range(s.pred_id, me.first.start, finger[[0]].succ)
        dest = s.pred_addr
      else
        dest = finger[[0]].succ_addr
      end
      [dest, me.first.start, ip_port]
    end
  end

  # handle predecessor notifications
  bloom :notify do
    # replace predecessor
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

    # when we receive keys being transfered, put them in localkeys and ack
    localkeys <= xfer_keys {|x| x.keyval }
    xfer_keys_ack <~ xfer_keys {|x| [x.sender, x.keyval, ip_port, me.first.start]}    

    # once key transfer succeeds, delete the transfered keys
    localkeys <- xfer_keys_ack do |x|
      # puts "deleting #{x.keyval.inspect}" if in_range(x.keyval[0], x.ack_start, me.first.start, true, false)
      x.keyval if in_range(x.keyval[0], me.first.start, x.ack_start, false, true)
    end            
  end

  # periodically fix up fingers
  bloom :fix_dem_fingers do
    # try to fix any nil fingers
    fix_finger_finder.succ_req <= (fix_fingers * finger).pairs do |fix, fing|
      [fing.start] if fing.succ.nil?
    end
    
    # each period, fix a random finger

    # hackage: produce a singleton with a random value.
    # we'd like this next line to produce a singleton set, but the worry is that the
    # evaluator may call the rhs multiple times, so we also run an argagg on the result
    # to pick randomly among the random values each time.
    rands <= [[rand(log2(@maxkey))]]
    rand_ix <= rands.argagg(:choose_rand, [], :val)

    # find successor of the random finger's start value
    fix_finger_finder.succ_req <= fix_fingers do |f|
      unless rand_ix.length == 0 or finger[ [rand_ix.first.val] ].nil? or finger[ [rand_ix.first.val] ].start.nil? or finger[ [rand_ix.first.val] ].succ.nil?
        [finger[ [rand_ix.first.val] ].start]        
      end
    end
    
    # upon response to successor lookup, replace finger
    finger <+ (fix_finger_finder.succ_resp * finger).pairs(:key => :start) do |s,f|
      [f.index, f.start, f.hi, s.start, s.addr] unless s.start == f.succ
    end
    finger <- (fix_finger_finder.succ_resp * finger).pairs(:key => :start) do |s,f|
      f unless s.start == f.succ
    end
  end
end