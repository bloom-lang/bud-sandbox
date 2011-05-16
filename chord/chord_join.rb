require 'rubygems'
require 'bud'
require 'chord/chord_find'

module ChordJoin
  import ChordFind => :join_finder
  import ChordFind => :upd_others_finder
  import ChordFind => :fix_finger_finder

  state do
    interface input, :join_up, [:to, :start]
    channel :join_req, [:@to, :requestor_addr, :start]
    table   :join_pending, join_req.schema
    # table :finger, [:index] => [:start, 'hi', :succ, :succ_addr]
    # interface output, :succ_resp, [:key] => [:start, :addr]
    channel :finger_table_req, [:@to,:requestor_addr]
    channel :finger_table_resp, [:@requestor_addr] + finger.key_cols => finger.val_cols
    channel :node_pred_resp, [:@requestor_addr, :start, :pred_id, :pred_addr, :from]
    channel :pred_req, [:@referrer_addr, :referrer_key, :referrer_index]
    channel :pred_resp, [:@requestor_addr, :referrer_key, :referrer_index, :referrer_addr]
    channel :finger_upd, [:@referrer_addr, :referrer_index, :my_start, :my_addr]
    channel :succ_upd_pred, [:@to, :pred_id, :addr]
    table :offsets, [:val]
    scratch :got_fingers, [:val]
    table :done_fingers, got_fingers.schema
    
    channel :xfer_keys_ack, [:@ackee, :keyval, :acker, :ack_start]
    channel :xfer_keys, [:@receiver, :keyval, :sender]
  end

  bootstrap do
    offsets <= (1..log2(@maxkey)).map{|o| [o]}
  end

  bloom :join_rules_at_proxy do
    # an existing member serves as a proxy for the new node that wishes to join.
    # when it receives a join req from new node, it requests successors on the new
    # node's behalf

    # send out the join request
    join_req <~ join_up {|j| [j.to, ip_port, j.start]}
    # cache the request
    join_pending <= join_req
    
    # asynchronously, find out who owns start+1
    join_finder.succ_req <= join_req.map{|j| [j.start+1]}
    
    # upon response to successor request, ask the successor to send the contents
    #  of its finger table directly to the new node.
    finger_table_req <~ (join_pending * join_finder.succ_resp).pairs do |j, s|
      [s.addr, j.requestor_addr] if j.start+1 == s.key
    end
    
    temp :f_reqs <= (join_pending * join_finder.succ_resp).pairs do |j, s|
      [s.addr, j.requestor_addr] if j.start+1 == s.key
    end
    temp :f_req_cnt <= f_reqs.group([], count)
    
    # stdio <~ (join_pending * join_finder.succ_resp).pairs do |j, s|
    #   ["found successor " + [s.addr, j.requestor_addr].inspect] if j.start+1 == s.key
    # end
  end

  bloom :join_rules_at_successor do
    # at successor, upon receiving finger_table_req, ship finger table entries directly to new node
    finger_table_resp <~ (finger_table_req * finger).pairs do |ftreq, f|
      # finger tuple prefixed with requestor_addr
      [ftreq.requestor_addr] + f
    end
    # also manufacture a finger entry pointing to me for new node to consider
    finger_table_resp <~ (finger_table_req * me).pairs do |f, m|
      [f.requestor_addr, -1, m.start, (m.start+1 % @maxkey), m.start, ip_port]
    end
    # and ship predecessor info directly to new node
    node_pred_resp <~ (finger_table_req * me).pairs do |f, m|
      [f.requestor_addr, m.start, m.pred_id, m.pred_addr, ip_port]
    end
    
    # and when new predecessor responds with succ_upd_pred, we set our predecessor to it
    me <+ (me * succ_upd_pred).pairs do |m, s|
      # puts "#{ip_port} updating me to #{[m.start, s.pred_id, s.addr].inspect}"
      [m.start, s.pred_id, s.addr]
    end
    me <- (me * succ_upd_pred).lefts 
        
    # and we initiate key transfer
    xfer_keys <~ (me * succ_upd_pred * localkeys).combos do |m,s,l|
      [s.addr, l, ip_port] if in_range(l.key, s.pred_id, m.start, true, false)
    end
    
    # stdio <~ (me * succ_upd_pred * localkeys).combos do |m,s,l|
    #   ["initiating key xfer #{[s.addr, l, ip_port].inspect}"] if in_range(l.key, s.pred_id, m.start, true, false)
    # end
      
    # once key transfer succeeds, delete the transfered keys
    localkeys <- (xfer_keys_ack * me).pairs do |x, m|
      # puts "deleting #{x.keyval.inspect}" if in_range(x.keyval[0], x.ack_start, m.start, true, false)
      x.keyval if in_range(x.keyval[0], x.ack_start, m.start, true, false)
    end        
  end

  bloom :join_rules_at_new_node do
    # # at new member, install successor in finger table
    # finger <= (me * node_pred_resp).pairs do |m,n|
    #   [0, (m.start + 1) % @maxkey, (m.start+2) % @maxkey, n.start, n.from]
    # end
    # 
    # install non-successor fingers based on entries from successor
    finger <+ (finger_table_resp * offsets * me).pairs do |f,o,m|
      if in_range((m.start + 2**(o.val-1)) % @maxkey, f.start, f.hi, true, false)
        [o.val - 1, (m.start + 2**(o.val-1)) % @maxkey, (m.start + 2**o.val) % @maxkey, f.succ, f.succ_addr] 
      end
    end
    got_fingers <= finger_table_resp { |f| [true] if done_fingers.empty? and finger.length > (2*offsets.length / 3)} 
    done_fingers <+ got_fingers
    
    # and start fixing our fingers
    fix_finger_finder.succ_req <= (me * offsets * got_fingers).combos do |m,o,g|
      [m.start + 2**(o.val - 1)]
    end
    # table :finger, [:index] => [:start, :hi, :succ, :succ_addr]
    # interface output, :succ_resp, [:key] => [:start, :addr]
    
    finger <+ (fix_finger_finder.succ_resp * finger * me).combos do |fix, fing, m|
      if ((m.start + 2**(fing.index)) % @maxkey) == fix.key
        newfing = [fing.index, fing.start, fing.hi, fix.start, fix.addr] 
        unless newfing == fing
          newfing
        end
      end
    end
    finger <- (fix_finger_finder.succ_resp * finger * me).combos do |fix, fing, m|
      newfing = [fing.index, fing.start, fing.hi, fix.start, fix.addr] 
      if ((m.start + 2**(fing.index)) % @maxkey) == fix.key and newfing != fing
        fing 
      end
    end
    
    # update my predecessor info; ignore if successor's predecessor is me!
    me <+ (me * node_pred_resp).pairs do |m,n|
      [m.start, n.pred_id, n.pred_addr] if m.pred_id.nil? and not n.pred_id == m.start
    end
    me <- (me * node_pred_resp).pairs {|m,n| m unless n.pred_id == m.start}
    
    succ_upd_pred <~ (node_pred_resp * me).pairs do |n, m|
      [n.from, m.start, ip_port]
    end

    # once fingers are installed, initiate update_others():
    # XXX Actually we should fix our fingers first. XXX
    # update all nodes whose finger tables should refer here
    # first, for each offset o find last node whose o'th finger might be the new node's id
    
    upd_others_finder.pred_req <= (me * offsets * got_fingers).combos do |m,o,g|
      [(m.start - 2**(o.val-1)) % @maxkey]
    end

    # stdio <~ upd_others_finder.pred_req {|p| ["finding #{p.inspect}"]}
    # upon pred_resp, send a finger_upd message to the node that was found to point here
    finger_upd <~ (me * offsets * upd_others_finder.pred_resp).pairs do |m, o, resp|
      [resp.addr, o.val-1, m.start, ip_port] if resp.key == ((m.start - 2**(o.val-1)) % @maxkey)
    end
    
    # stdio <~ finger_upd {|f| ["sending finger_upd #{f.inspect}"]}
    
    # when we receive keys being transfered, put them in localkeys
    localkeys <= xfer_keys {|x| x.keyval }
    # stdio <~ xfer_keys {|x| ["got key xfer #{x.inspect}"]}
    # and ack
    xfer_keys_ack <~ (xfer_keys * me).pairs {|x, m| [x.sender, x.keyval, ip_port, m.start]}    
  end

  bloom :join_rules_at_others do
    # upon a finger_upd, update fingers if the new one works: insert new, delete old
    # XXX would be nice to have an update pattern in Bloom for this
    temp :k <= (finger_upd * finger).pairs(:referrer_index => :index) 
    # stdio <~ k do |u,f| 
    #   ["applying finger_upd: #{u.inspect}"] if in_range(u.my_start, f.start, f.succ)
    # end
    
    finger <+ k do |u,f|
      [f.index, f.start, f.hi, u.my_start, u.my_addr] if in_range(u.my_start, f.start, f.succ)
    end
    # stdio <~ k do |u, f|
    #   ["at #{ip_port} deleting finger #{f.inspect}"] if in_range(u.my_start, f.start, f.succ)
    # end
    finger <- k do |u, f|
      f if in_range(u.my_start, f.start, f.succ)
    end
    # # and forward to predecessor if it worked here
    finger_upd <~ (finger_upd * finger * me).combos(:referrer_index => :index) do |u,f,m| 
      [m.pred_addr, u.referrer_index, u.my_start, u.my_addr] if in_range(u.my_start, f.start, f.succ)
    end
    
  end
end
