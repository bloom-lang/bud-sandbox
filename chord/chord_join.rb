require 'rubygems'
require 'bud'
require 'chord_find'

module ChordJoin
  include Anise
  annotator :declare
  include ChordFind
  
  def state
    super
    channel :join_req, ['from'], ['start']
    table   :join_pending, join_req.keys, join_req.cols
    # table :finger, ['index'], ['start', 'hi', 'succ', 'succ_addr']
    # interface output, :succ_resp, ['key'], ['start', 'addr']        
    channel :finger_table_req, ['@to','requestor_addr']
    channel :finger_table_resp, ['@requestor_addr'] + finger.keys, finger.cols
    channel :pred_req, ['referrer_key', 'referrer_index']
    channel :pred_resp, ['referrer_key', 'referrer_index', 'referrer_addr']
    channel :finger_upd, ['referrer_addr', 'referrer_index', 'my_start', 'my_addr']
    table :offsets, ['val']
  end
  
  def log2(x)
    log(x)/log(2)
  end

  def initialize
    super
    offsets <= [1..log2(@maxkey)]
  end
  
  
  declare
  def join_rules_proxy
    # an existing member serves as a proxy for the new node that wishes to join. 
    # when it receives a join req from new node, it requests successors on the new 
    # node's behalf
    
    # cache the request
    join_pending <= join_req
    # asynchronously, find out who owns start+1
    succ_req <= join_req.map{|j| j.start+1} 
    # upon response to successor request, ask the successor to send the contents
    #  of its finger table directly to the new node.  
    finger_table_req <~ join([join_pending, succ_resp], 
                             [join_pending.start+1, succ_resp.key]).map do |j, s|
      [s.addr, j.from]
    end
  end
  
  declare 
  def join_rules_successor
    # at successor, upon receiving finger_table_req, ship finger table entries directly to new node
    finger_table_resp <~ join([finger_table_req, finger]).map do |ft, f|
      # finger tuple prefixed with requestor_addr
      [ft.requestor_addr] + f
    end
  end
  
  declare
  def join_rules_new_node
    # at new member, install finger entries
    finger <= finger_table_resp do |f|
      # construct tuple that contains all the finger columns in f
      (finger.keys + finger.cols).map{|c| f.send{c.to_sym}}
    end
    # update all nodes whose finger tables should refer here
    # first, for each offset o find last node whose o'th finger might be n
    pred_req <= join([me,offsets]).map do |m,o|
      [m.start - 2**o.val, o.val]
    end
    finger_upd <~ natjoin([me, pred_resp]).map do |m, resp|
      [resp.referrer_addr, resp.referrer_index, m.start, @ip_port]
    end
  end
  
  declare
  def join_rules_referers
    # update finger entries upon a finger_upd if the new one works: insert new, delete old
    # XXX would be nice to have an update pattern in Bloom for this
    finger <+ join([finger_upd, finger], [finger_upd.referrer_index, finger.index]).map do |u, f|
      [f.index, f.start, f.hi, u.my_start, u.from] if in_range(u.my_start, f.start, f.hi)
    end
    finger <- join([finger_upd, finger], [finger_upd.referrer_index, finger.index]).map do |u, f|
      f if in_range(u.my_start, f.start, f.hi)
    end
  end
end
