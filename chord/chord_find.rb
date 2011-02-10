require 'rubygems'
require 'bud'
require 'chord_node'

module ChordFind
  include Anise
  annotator :declare
  
  include ChordNode
  def state
    super
    interface input, :succ_req, ['key']
    interface output, :succ_resp, ['key'], ['start', 'addr']    
    
    channel :find_req, ['@dest', 'key', 'from']
    channel :find_resp, ['@dest', 'key'], ['start', 'addr']      
  end
  
  def at_successor(event, me, fing)
    fing.index == 0 and in_range(event.key, me.start, fing.succ, true)
  end
  
  declare 
  def find_recursive
    # convert local successor requests into local find_events
    find_event <= succ_req.map{|s| [s.key, ip_port]}
    
#    stdio <~ find_req.map{|f| [["#{port} got find_req #{f.inspect}"]]}
    
    # convert incoming find_req messages into local find_events
    find_event <= find_req.map{|f| [f.key, f.from]}
    
    # if not at successor, forward to closest finger   
    find_req <~ join([find_event, finger, closest, me], [find_event.key, closest.key]).map do |e, f, c, m| 
      # stdio <~ [["#{m.start}: forwarding #{e.key} from #{e.from} to closest finger, #{c.succ_addr}!"]] unless at_successor(e,m,f)
      [c.succ_addr, e.key, e.from] unless at_successor(e,m,f)
    end

    # else at successor, so respond with successor's ID/address
    find_resp <~ join([find_event, finger, me]).map do |e, f, m|
      # stdio <~ [["#{m.start}: #{e.key} req from #{e.from} found at successor #{f.succ_addr}!"]] if at_successor(e,m,f)
      [e.from, e.key, f.succ, f.succ_addr] if at_successor(e,m,f)
    end
    
    # when we receive a response, put it to the output interface
    succ_resp <= find_resp.map { |f| [f.key, f.start, f.addr] }
  end
end
