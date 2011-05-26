require 'rubygems'
require 'bud'
require 'chord/chord_find'
require 'chord/chord_stable'

module ChordSuccessors
  state do
    table   :other_succs, [:start, :addr]
    scratch :succ_dist,   [:start, :addr, :dist]
    scratch :successors, other_succs.schema
    scratch :min_other_succ, other_succs.schema
    table   :succ_pred_pending, [:addr, :timestep]
    scratch :successor_fail, [:addr]
    scratch :sfail,          [:addr]
  end
  
  bloom :form_successors do
    successors <= other_succs
    successors <= me { |m| [finger[0].succ, finger[0].succ_addr] unless finger[0].nil? or finger[0].succ.nil?}
  end
  
  bloom :stabilize_successors do
    # ask each successor for its predecessor
    temp :spr <= (stable_timer * other_succs).rights do |s| 
      [s.addr, ip_port] unless s.addr.nil?
    end
    # stdio <~ spr.inspected
    succ_pred_req <~ spr
    succ_pred_pending <= spr{|s| [s[0], budtime]}
    
    # ChordStable has the logic to handle the succ_pred_req calls and produce succ_pred_resp
  
    # upon response
    # remove pending
    succ_pred_pending <- (succ_pred_pending * succ_pred_resp).lefts(:addr => :from)
    # if not finger[0], then rememember in other_succs
    other_succs <= succ_pred_resp do |s| 
      unless in_range(s.pred_id, me.first.start, finger[[0]].succ)
        [s.pred_id, s.pred_addr]
      end
    end

    # timeout after 5 secs
    sfail <= (succ_pred_pending * stable_timer).pairs do |p,t|
      [p.addr] if (Time.now - t.val) > 5
    end
    successor_fail <= sfail
    succ_pred_pending <- (sfail * succ_pred_pending).rights(:addr => :addr)
    
    # successor_fail catches timeouts on succ_pred_req msgs, and 
    # swaps in a new finger[0] as needed
    succ_dist <= other_succs do |o| 
      unless me.nil? or me.first.nil? or me.first.start.nil? or o.start.nil?
        [o.addr, o.start, (o.start - me.first.start) % @maxkey]
      end
    end
    min_other_succ <= succ_dist.argmin([], :dist) { |o| [o[0], o[1]] }
    finger <+ (successor_fail * min_other_succ).pairs do |s, m| 
      if s.addr == finger[0].succ_addr
        [0, finger[0].start, finger[0].hi, m.start, m.addr]
      end
    end
    finger <- successor_fail do |s|
      finger[0] if s.addr == finger[0].succ_addr
    end
  end
end