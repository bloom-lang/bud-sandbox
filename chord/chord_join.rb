require 'rubygems'
require 'bud'
require 'chord/chord_find'

# This module for reference/experimentation only! 
# It is the "algorithm of Section 4" from the paper, 
# superceded by the stabilization protocol presented in Section 5 
# (and in chord_stable.rb here).
#
# Do not import this module and ChordStable together!
#
# As an experiment in design style, the rules here are batched by a node's "role", 
# rather than by the pseudocode structure in the paper.
module ChordJoin
  import ChordFind => :join_finder
  import ChordFind => :upd_others_finder

  state do
    interface input, :join_up, [:to, :start]
    channel :join_req, [:@to, :requestor_addr, :start]
    table   :join_pending, join_req.schema
    channel :finger_table_req, [:@to,:requestor_addr]
    channel :finger_table_resp, [:@requestor_addr] + finger.key_cols => finger.val_cols
    channel :node_pred_resp, [:@requestor_addr, :start, :pred_id, :pred_addr, :from]
    channel :pred_req, [:@referrer_addr, :referrer_key, :referrer_index]
    channel :pred_resp, [:@requestor_addr, :referrer_key, :referrer_index, :referrer_addr]
    channel :finger_upd, [:@referrer_addr, :referrer_index, :my_start, :my_addr]
    channel :succ_upd_pred, [:@to, :pred_id, :addr]
    table :offsets, [:val]
    scratch :new_finger, finger.schema
    
    channel :xfer_keys_ack, [:@ackee, :keyval, :acker, :ack_start]
    channel :xfer_keys, [:@receiver, :keyval, :sender]
  end

  # precompute the set of finger offsets
  bootstrap do
    offsets <= (1..log2(@maxkey)).map{|o| [o]}
  end


  # an existing member serves as a proxy for the new node that wishes to join.
  # when it receives a join req from new node, it requests successors on the new
  # node's behalf
  bloom :join_rules_at_proxy do
    # cache the request
    join_pending <= join_req
    
    # asynchronously, find out who owns start+1
    join_finder.succ_req <= join_req.map{|j| [j.start+1]}
    
    # upon response to successor request, ask the successor to send the contents
    #  of its finger table directly to the new node.
    finger_table_req <~ (join_pending * join_finder.succ_resp).pairs do |j, s|
      [s.addr, j.requestor_addr] if j.start+1 == s.key
    end    
  end

  # the successor to the new node participates by bootstrapping the new node
  # and connecting with it to change the ring
  bloom :join_rules_at_successor do
    # "practical optimization" from Section 4.4 of paper:
    # upon receiving finger_table_req, ship finger table entries directly to new node
    # to pre-populate its finger table
    finger_table_resp <~ (finger_table_req * finger).pairs do |ftreq, f|
      # finger tuple prefixed with requestor_addr
      [ftreq.requestor_addr] + f
    end
    # also manufacture a finger entry pointing to me for new node to consider
    finger_table_resp <~ finger_table_req do |f|
      unless finger[[0]].nil? or finger[[0]].start.nil? or me.first.nil?        
        [f.requestor_addr, -1, me.first.start, finger[[0]].start, me.first.start, ip_port]
      end
    end
    # and ship current predecessor info directly to new node
    node_pred_resp <~ finger_table_req do |f|
      unless me.first.nil?
        [f.requestor_addr, me.first.start, me.first.pred_id, me.first.pred_addr, ip_port]
      end
    end
    
    # when new predecessor responds with succ_upd_pred, set our predecessor to the new node
    me <+ succ_upd_pred do |s|
      [me.first.start, s.pred_id, s.addr]
    end
    me <- (me * succ_upd_pred).lefts 
    # and initiate transfer of keys that now belong to new node
    xfer_keys <~ (me * succ_upd_pred * localkeys).combos do |m,s,l|
      [s.addr, l, ip_port] if in_range(l.key, s.pred_id, m.start, true, false)
    end
    
    # once key transfer succeeds, delete the transfered keys
    localkeys <- (xfer_keys_ack * me).pairs do |x, m|
      # puts "deleting #{x.keyval.inspect}" if in_range(x.keyval[0], x.ack_start, m.start, true, false)
      x.keyval if in_range(x.keyval[0], x.ack_start, m.start, true, false)
    end        
  end

  # logic for the new node, which needs to communicate with a proxy, its successor,
  # and any node that should have a finger pointing to the new node
  bloom :join_rules_at_new_node do
    # to begin, send join request to the proxy and initialize fingers to defaults.
    join_req <~ join_up {|j| [j.to, ip_port, j.start]}
    finger <= (join_up * offsets * me).combos do |j,o,m|
      [o.val - 1, (m.start + 2**(o.val-1)) % @maxkey, (m.start + 2**o.val) % @maxkey, nil, nil] unless o.val==1
    end
    finger <= (join_up * offsets * me).combos do |j,o,m|
      [0, (m.start + 1) % @maxkey, (m.start + 2) % @maxkey, 0, j.to]
    end
    
    # when we get finger_table responses from successor, overwrite the old naive entries
    finger <+ (finger_table_resp * offsets * me).combos do |f,o,m|
      if in_range((m.start + 2**(o.val-1)) % @maxkey, f.start, f.hi, true, false)
        [o.val - 1, (m.start + 2**(o.val-1)) % @maxkey, (m.start + 2**o.val) % @maxkey, f.succ, f.succ_addr]
      end
    end
    finger <- (finger_table_resp * offsets * me * finger).combos do |f,o,m,fing|
      if in_range((m.start + 2**(o.val-1)) % @maxkey, f.start, f.hi, true, false) and (o.val-1) == fing.index
        fing
      end
    end    
    
    # link into the ring: when successor responds with its predecessor,
    # update my predecessor info.
    # ignore if successor's predecessor is me!
    me <+ (me * node_pred_resp).pairs do |m,n|
      [m.start, n.pred_id, n.pred_addr] if m.pred_id.nil? and not n.pred_id == m.start
    end
    me <- (me * node_pred_resp).pairs {|m,n| m unless n.pred_id == m.start}
    
    succ_upd_pred <~ (node_pred_resp * me).pairs do |n, m|
      [n.from, m.start, ip_port]
    end

    # once fingers_table_resps start coming in, give them a tick to install, and then
    # update all nodes whose finger tables should refer here.
    # first, for each offset o find last node whose o'th finger might be the new node's id
    upd_others_finder.pred_req <+ (offsets * finger_table_resp).lefts do |o|
      [(me.first.start - 2**(o.val-1)) % @maxkey] unless me.first.nil?
    end
    # as "others" are identified, send them a finger_upd message to point here
    finger_upd <~ (me * offsets * upd_others_finder.pred_resp).pairs do |m, o, resp|
      [resp.addr, o.val-1, m.start, ip_port] if resp.key == ((m.start - 2**(o.val-1)) % @maxkey)
    end
    
    # when we receive keys being transfered from successor, put them in localkeys and ack
    localkeys <= xfer_keys {|x| x.keyval }
    xfer_keys_ack <~ (xfer_keys * me).pairs {|x, m| [x.sender, x.keyval, ip_port, m.start]}    
  end

  bloom :join_rules_at_others do
    # upon a finger_upd from a new node, update fingers if the new one works
    new_finger <= (finger_upd * finger).pairs(:referrer_index => :index) do |u,f|
      [f.index, f.start, f.hi, u.my_start, u.my_addr] if in_range(u.my_start, f.start, f.succ)
    end
    finger <- (new_finger * finger).rights(:index => :index)
    finger <+ new_finger
    
    # and forward to predecessor if it worked here
    finger_upd <~ (finger_upd * new_finger).lefts(:referrer_index => :index) do |u| 
      unless me.first.nil? or me.first.pred_addr.nil?
        [me.first.pred_addr, u.referrer_index, u.my_start, u.my_addr] 
      end
    end
    
  end
end
