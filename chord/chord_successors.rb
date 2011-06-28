require 'rubygems'
require 'bud'
require 'chord/chord_find'
require 'chord/chord_node'

# support multiple successors per node.
# this is independent of whether you use ChordStable or ChordJoin
module ChordSuccessors
  import ChordSuccPred => :sp2
  
  state do
    table   :other_succs, [:index] => [:start, :addr, :dist]
    scratch :mos, other_succs.schema
    scratch :successors, other_succs.schema
    scratch :min_other_succ, other_succs.schema
    table   :succ_pred_pending, [:addr, :timestep, :hops]
    scratch :successor_fail, [:addr]
    scratch :sfail,          [:addr]
    periodic :succ_timer, 2
  end
  
  bloom :form_successors do
    successors <= other_succs
    successors <= me { |m| [0, finger[[0]].succ, finger[[0]].succ_addr, (finger[[0]].start - me.first.start) % @maxkey] unless finger[[0]].nil? or finger[[0]].succ.nil?}
  end
  
  bloom :stabilize_successors do
    # ask successor for a bunch of successor_predecessors
    temp :spr <= succ_timer do |t| 
      [finger[[0]].succ_addr, ip_port, log2(@maxkey)] unless finger[[0]].nil? or finger[[0]].succ_addr.nil?
    end
    sp2.succ_pred_req <= spr
    succ_pred_pending <= spr{|s| [s[0], budtime, s[2]]}
    
    # upon response, remove pending
    succ_pred_pending <- (succ_pred_pending * sp2.succ_pred_resp).lefts(:addr => :from)
    # and if not finger[[0]], then rememember in other_succs
    other_succs <+ sp2.succ_pred_resp do |s| 
      unless me.first.nil? or finger[[0]].nil? or s.hops == log2(@maxkey)
        [(log2(@maxkey) - s.hops).to_i, s.start, s.from, (s.start - me.first.start) % @maxkey]
      end
    end
    other_succs <- (sp2.succ_pred_resp * other_succs).combos { |s,o| o if (log2(@maxkey) - s.hops).to_i == o.index }
    
    # on sp2.succ_pred_timeout, swap in a new finger[[0]] as needed
    mos <= (succ_timer * other_succs).rights
    min_other_succ <= mos.argmin([], :dist)
    finger <+ (sp2.succ_pred_timeout * min_other_succ).pairs do |s, m| 
      if finger[[0]] and s.to == finger[[0]].succ_addr
        [0, finger[[0]].start, finger[[0]].hi, m.start, m.addr]
      end
    end
    finger <- sp2.succ_pred_timeout do |s|
      finger[[0]] if finger[[0]] and s.to == finger[[0]].succ_addr
    end
  end
end