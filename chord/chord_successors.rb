require 'rubygems'
require 'bud'
require 'chord/chord_find'
require 'chord/chord_stable'

# support multiple successors per node.
# this is intended to be included along with ChordStable
module ChordSuccessors
  import ChordSuccPred => :sp2
  
  state do
    table   :other_succs, [:index] => [:start, :addr, :dist]
    scratch :successors, other_succs.schema
    scratch :min_other_succ, other_succs.schema
    table   :succ_pred_pending, [:addr, :timestep, :hops]
    scratch :successor_fail, [:addr]
    scratch :sfail,          [:addr]
  end
  
  bloom :form_successors do
    successors <= other_succs
    successors <= me { |m| [finger[[0]].succ, finger[[0]].succ_addr, (finger[[0]].start - me.first.start) % @maxkey] unless finger[[0]].nil? or finger[[0]].succ.nil?}
  end
  
  bloom :stabilize_successors do
    # ask each successor for its predecessor
    temp :spr <= (stable_timer * other_succs).rights do |s| 
      [s.addr, ip_port, log2(@maxkey)] unless s.addr.nil?
    end
    # stdio <~ spr.inspected
    sp2.succ_pred_req <= spr
    succ_pred_pending <= spr{|s| [s[0], budtime]}
    
    # upon response, remove pending
    succ_pred_pending <- (succ_pred_pending * sp2.succ_pred_resp).lefts(:addr => :from)
    # and if not finger[[0]], then rememember in other_succs
    other_succs <+ sp2.succ_pred_resp do |s| 
      unless me.first.nil? or finger[[0]].nil? or in_range(s.pred_id, me.first.start, finger[[0]].succ)
        [log2(@maxkey) - s.hop_num, s.pred_id, s.pred_addr, (me.first.start - s.pred_id) % @maxkey]
      end
    end
    other_succs <- (sp2.succ_pred_resp * other_succs).combos { |s,o| o if log2(@maxkey) - s.hop_num == o.index }
    
    # timeout after 5 secs
    sfail <= (succ_pred_pending * stable_timer).pairs do |p,t|
      [p.addr] if (Time.now - t.val) > 5
    end
    successor_fail <= sfail
    succ_pred_pending <- (sfail * succ_pred_pending).rights(:addr => :addr)
    
    # successor_fail catches timeouts on succ_pred_req msgs, and 
    # swaps in a new finger[[0]] as needed
    min_other_succ <= other_succs.argmin([], :dist) { |o| [o[1], o[2]] }
    finger <+ (successor_fail * min_other_succ).pairs do |s, m| 
      if s.addr == finger[[0]].succ_addr
        [0, finger[[0]].start, finger[[0]].hi, m.start, m.addr]
      end
    end
    finger <- successor_fail do |s|
      finger[[0]] if s.addr == finger[[0]].succ_addr
    end
    
    # if other succs is too large, prune out the element furthest from here
    # compute size of other_succs
    temp :os_size <= other_succs.group([], count)
    other_succs <- other_succs.argmax([], :dist) do |o| 
      o if os_size.first and os_size.first[0] > log2(@maxkey)
    end
  end
end