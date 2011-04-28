require 'rubygems'
require 'bud'
require 'chord/chord_find'

module ChordJoin
  import ChordFind => :join_finder

  state do
    channel :join_req, [:@to, :requestor_addr] => [:start]
    table   :join_pending, join_req.schema
    # table :finger, [:index] => [:start, 'hi', :succ, :succ_addr]
    # interface output, :succ_resp, [:key] => [:start, :addr]
    channel :finger_table_req, [:@to,:requestor_addr]
    channel :finger_table_resp, [:@requestor_addr] + finger.key_cols => finger.val_cols
    channel :pred_req, [:@referrer_addr, :referrer_key, :referrer_index]
    channel :pred_resp, [:@requestor_addr, :referrer_key, :referrer_index, :referrer_addr]
    channel :finger_upd, [:@referrer_addr, :referrer_index, :my_start, :my_addr]
    table :offsets, [:val]
    table :ftable_req_cnt, [:cnt]
    table :ftable_resp_cnt, [:cnt]
  end

  bootstrap do
    offsets <= [(1..log2(@maxkey)).map{|o| o}]
  end

  bloom :join_rules_proxy do
    # an existing member serves as a proxy for the new node that wishes to join.
    # when it receives a join req from new node, it requests successors on the new
    # node's behalf

    # cache the request
    join_pending <= join_req
    # asynchronously, find out who owns start+1
    join_finder.succ_req <= join_req.map{|j| [j.start+1]}
    # upon response to successor request, ask the successor to send the contents
    #  of its finger table directly to the new node.
    finger_table_req <~ (join_pending * join_finder.succ_resp).pairs do |j, s|
      [s.addr, j.requestor_addr] if j.start+1 == s.key
    end
    
    temp :f_req_cnt <= (join_pending * join_finder.succ_resp).pairs do |j, s|
      [s.addr, j.requestor_addr] if j.start+1 == s.key
    end
    ftable_req_cnt <= f_req_cnt.group([], count)
    
    stdio <~ (join_pending * join_finder.succ_resp).pairs do |j, s|
      ["found successor " + [s.addr, j.requestor_addr].inspect] if j.start+1 == s.key
    end
  end

  bloom :join_rules_successor do
    # at successor, upon receiving finger_table_req, ship finger table entries directly to new node
    finger_table_resp <~ (finger_table_req * finger).pairs do |ftreq, f|
      # finger tuple prefixed with requestor_addr
      [ftreq.requestor_addr] + f
    end
  end

  bloom :join_rules_new_node do
    # at new member, install finger entries
    stdio <~ finger_table_resp do |f|
      ["got finger_table_resp " + f.inspect]
    end
    finger <= finger_table_resp.map do |f|
      # construct tuple that contains all the finger columns in f
      # [f.index, f.start, f.hi, f.succ, f.succ_addr]
      finger.schema.map{|c| f.send(c.to_sym)}
    end
    # update all nodes whose finger tables should refer here
    # first, for each offset o find last node whose o'th finger might be the new node's id
    ftable_resp_cnt <= finger_table_resp.group([], count)
    
    join_finder.succ_req <= (me * offsets * ftable_req_cnt * ftable_resp_cnt).combos do |m,o,f_req,f_resp|
      puts "f_req.cnt = #{f_req.cnt}, f_resp.cnt = #{f_resp.cnt}"
      puts "succ_req for [(m.start - 2**o.val) % @maxkey]" if f_req.cnt == f_resp.cnt
      [(m.start - 2**o.val) % @maxkey] if f_req.cnt == f_resp.cnt
    end
    pred_req <~ (me * offsets * join_finder.succ_resp).combos do |m,o,s|
      # puts [s.addr, s.key, o.val].inspect
      [s.addr, s.key, o.val]
    end
    # upon pred_resp, send a finger_upd message to the node that was found to point here
    finger_upd <~ (me * pred_resp).pairs do |m, resp|
      [resp.referrer_addr, resp.referrer_index, m.start, ip_port]
    end
  end

  bloom :join_rules_referers do
    # update finger entries upon a finger_upd if the new one works: insert new, delete old
    # XXX would be nice to have an update pattern in Bloom for this
    stdio <~ finger_upd.inspected
    temp :k <= (finger_upd * finger).pairs(:referrer_index => :index)
    stdio <~ k.inspected
    finger <+ k do |u, f|
      [f.index, f.start, f.hi, u.my_start, u.from] if in_range(u.my_start, f.start, f.hi)
    end
    finger <- k do |u, f|
      f if in_range(u.my_start, f.start, f.hi)
    end
  end
end
