require 'rubygems'
require 'bud'
require 'chord/chord_find'
require 'chord/chord_node'

# support multiple successors per node.
# this is independent of whether you use ChordStable or ChordJoin
module ChordSuccessors
  import ChordSuccPred => :sp2
  
  state do
    table   :successors, [:index] => [:start, :addr, :dist]
    scratch :mos, successors.schema
    scratch :min_successor, successors.schema
    table   :succ_pred_pending, [:addr, :timestep, :hops]
    scratch :succ_pred_dist, [:from, :dist, :start, :pred_id, :pred_addr]
    periodic :succ_timer, 2
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
    # and if not finger[[0]], then rememember in successors
    succ_pred_dist <= sp2.succ_pred_resp {|s| [s.from, (log2(@maxkey) - s.hops).to_i, s.start, s.pred_id, s.pred_addr]}
    successors <+ succ_pred_dist do |s| 
      unless me.first.nil? or finger[[0]].nil? or s.dist == 0
        [s.dist, s.start, s.from, (s.start - me.first.start) % @maxkey]
      end
    end
    successors <- (succ_pred_dist * successors).rights(:dist => :index)
    
    # on sp2.succ_pred_timeout, swap in a new finger[[0]] as needed
    mos <= (succ_timer * successors).rights
    min_successor <= mos.argmin([], :dist)
    finger <+- (sp2.succ_pred_timeout * min_successor).pairs do |s, m| 
      if finger[[0]] and s.to == finger[[0]].succ_addr
        [0, finger[[0]].start, finger[[0]].hi, m.start, m.addr]
      end
    end
  end
end