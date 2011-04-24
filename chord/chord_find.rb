require 'rubygems'
require 'bud'
require 'chord/chord_node'

module ChordFind
  state do
    interface input, :succ_req, [:key]
    interface output, :succ_resp, [:key] => [:start, :addr]    
    
    scratch :find_event, [:key, :from]
    scratch :candidate, [:key, :index, :start, :hi, :succ, :succ_addr]
    scratch :closest, [:key] => [:index, :start, :hi, :succ, :succ_addr]
    
    channel :find_req, [:@dest, :key, :from]
    channel :find_resp, [:@dest, :key] => [:start, :addr]      
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
    # convert local successor requests into local find_events
    find_event <= succ_req {|s| [s.key, ip_port]}
    
#    stdio <~ find_req {|f| [["#{port} got find_req #{f.inspect}"]]}
    
    # convert incoming find_req messages into local find_events
    find_event <= find_req {|f| [f.key, f.from]}
    
    # if not at successor, forward to closest finger   
    find_req <~ (find_event * finger * closest * me).combos(find_event.key => closest.key) do |e, f, c, m| 
      # stdio <~ [["#{m.start}: forwarding #{e.key} from #{e.from} to closest finger, #{c.succ_addr}!"]] unless at_successor(e,m,f)
      [c.succ_addr, e.key, e.from] unless at_successor(e,m,f)
    end

    # else at successor, so respond with successor's ID/address
    find_resp <~ (find_event * finger * me).combos do |e, f, m|
      # stdio <~ [["#{m.start}: #{e.key} req from #{e.from} found at successor #{f.succ_addr}!"]] if at_successor(e,m,f)
      [e.from, e.key, f.succ, f.succ_addr] if at_successor(e,m,f)
    end
    
    # when we receive a response, put it to the output interface
    succ_resp <= find_resp { |f| [f.key, f.start, f.addr] }
  end
end
