require 'rubygems'
require 'bud'
require 'chord/chord_node'

# Logic for doing key lookups in a Chord ring.
module ChordFind
  state do
    # External interfaces
    # Find successor of a key (the node responsible for the key)
    interface input, :succ_req, [:key]
    interface output, :succ_resp, [:key] => [:start, :addr]
    # Find predecessor of a key
    interface input, :pred_req, [:key]
    interface output, :pred_resp, [:key] => [:start, :addr]
    
    # private state
    scratch :find_event, [:key, :from, :pred_or_succ]
    scratch :candidate, [:key, :index, :start, :hi, :succ, :succ_addr]
    scratch :closest, [:key] => [:index, :start, :hi, :succ, :succ_addr]
    
    # private communication channels
    channel :find_req, [:@dest, :key, :from, :pred_or_succ]
    channel :find_resp, [:@dest, :key, :pred_or_succ] => [:start, :addr]      
  end
  
  # for each find_event, we find the index of the local finger closest to find_event.key.
  bloom :get_closest do
    # we begin by finding all fingers with IDs between this node and the search key
    candidate <= (find_event * finger * me).combos do |e,f,m|
                   [e.key, f.index, f.start, f.hi, f.succ, f.succ_addr] if (not f.succ.nil?) and in_range(f.succ, m.start, e.key)
                 end
    # then pick the highest-index candidate; it's the closest
    closest <= candidate.argmax([candidate.key], candidate.index)
    
    # if no candidates found, just send to successor
    closest <= find_event do |e| 
      f = finger[[0]]
      if candidate.length == 0 and not f.nil? and not f.succ.nil?
        [e.key, f.index, f.start, f.hi, f.succ, f.succ_addr] 
      end
    end
  end
  
  # resolve the find request.  If not local, forward the request to nearest finger.
  # the chord people call this "recursive" lookup in Section 6.1.
  bloom :find_recursive do
    # Merge local req's and inbound net req's into a single scratch.
    # first merge local requests into local find_events
    find_event <= pred_req {|s| [s.key, ip_port, 'pred']}
    find_event <= succ_req {|s| [s.key, ip_port, 'succ']}
    # and convert network-delivered find_req messages into local find_events
    find_event <= find_req {|f| [f.key, f.from, f.pred_or_succ]}
    
    # we can respond locally for keys that are here or at our successor
    find_resp <~ find_event do |e|
      start = nil    
      # if at successor:
      if at_successor(e.key)   
        if e.pred_or_succ == 'pred'
          start = me.first.start; addr = ip_port
        elsif e.pred_or_succ == 'succ' and finger[[0]] and not finger[[0]].succ.nil?
          start = finger[[0]].succ; addr = finger[[0]].succ_addr
        end
      # else if local:
      elsif at_local(e.key)
        if e.pred_or_succ == 'pred'
          start = me.first.pred_id; addr = me.first.pred_addr
        elsif e.pred_or_succ == 'succ'
          start = me.first.start; addr = ip_port
        end
      end
      [e.from, e.key, e.pred_or_succ, start, addr] unless start.nil?
    end
    
    # for keys that are not at successor, forward to closest finger   
    find_req <~ (find_event * closest).combos(:key => :key) do |e, c| 
      [c.succ_addr, e.key, e.from, e.pred_or_succ] unless at_successor(e.key) or at_local(e.key)
    end
    
    # when we receive a response, put it to the output interface
    succ_resp <= find_resp { |f| [f.key, f.start, f.addr] if f.pred_or_succ == 'succ'}
    pred_resp <= find_resp { |f| [f.key, f.start, f.addr] if f.pred_or_succ == 'pred'}
  end
end
