require 'rubygems'
require 'bud'
require 'chord/chord_node'

module ChordFind
  state do
    interface input, :succ_req, [:key]
    interface output, :succ_resp, [:key] => [:start, :addr]
    interface input, :pred_req, [:key]
    interface output, :pred_resp, [:key] => [:start, :addr]
    
    scratch :find_event, [:key, :from, :pred_or_succ]
    scratch :candidate, [:key, :index, :start, :hi, :succ, :succ_addr]
    scratch :closest, [:key] => [:index, :start, :hi, :succ, :succ_addr]
    
    channel :find_req, [:@dest, :key, :from, :pred_or_succ]
    channel :find_resp, [:@dest, :key, :pred_or_succ] => [:start, :addr]      
  end
  
  bloom :node_views do
    # for each find_event for an id, find index of the closest finger
    # start by finding all fingers with IDs between this node and the search key
    candidate <= (find_event * finger * me).combos do |e,f,m|
                   [e.key, f.index, f.start, f.hi, f.succ, f.succ_addr] if in_range(f.succ, m.start, e.key)
                 end
    # now for each key pick the highest-index candidate; it's the closest
    closest <= candidate.argmax([candidate.key], candidate.index)
    # stdio <~ closest {|m| ["closest@#{me.first.start.to_s}: #{m.inspect}"]}
  end
  
  bloom :find_recursive do
    # convert local requests into local find_events
    find_event <= pred_req {|s| [s.key, ip_port, 'pred']}
    find_event <= succ_req {|s| [s.key, ip_port, 'succ']}
    
    # convert incoming find_req messages into local find_events
    find_event <= find_req {|f| [f.key, f.from, f.pred_or_succ]}
    
    # if not at successor, forward to closest finger   
    find_req <~ (find_event * finger * closest * me).combos(find_event.key => closest.key) do |e, f, c, m| 
       # stdio <~ [["#{m.start}: forwarding #{e.key} from #{e.from} to closest finger, #{c.succ_addr}!"]] unless at_successor(e,m,f) or e.from == ip_port
      [c.succ_addr, e.key, e.from, e.pred_or_succ] unless at_successor(e,m,f)
    end

    # if at successor, respond accordingly
    find_resp <~ (find_event * finger * me).combos do |e, f, m|
      # stdio <~ [["#{m.start}: #{e.key} req from #{e.from} found at successor #{f.succ_addr}!"]] if at_successor(e,m,f)
      if at_successor(e,m,f)
        if e.pred_or_succ == 'pred'
          [e.from, e.key, e.pred_or_succ, m.start, ip_port] 
        elsif e.pred_or_succ == 'succ'
          [e.from, e.key, e.pred_or_succ, f.succ, f.succ_addr]
        end
      else
        nil
      end
    end

    # if local, respond accordingly.  (The forwarding logic won't match!)
    find_resp <~ (find_event * me).pairs do |e, m|
      if e.key == m.start
        if e.pred_or_succ == 'pred'
          [e.from, e.key, e.pred_or_succ, m.pred_id, m.pred_addr]
        elsif e.pred_or_succ == 'succ'
          [e.from, e.key, e.pred_or_succ, m.start, ip_port]
        end
      end
    end
    
    # when we receive a response, put it to the output interface
    succ_resp <= find_resp { |f| [f.key, f.start, f.addr] if f.pred_or_succ == 'succ'}
    pred_resp <= find_resp { |f| [f.key, f.start, f.addr] if f.pred_or_succ == 'pred'}
  end
end
