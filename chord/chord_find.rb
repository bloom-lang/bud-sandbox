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
    
  declare 
  def find_recursive
    # convert local successor requests into local find_events
    find_event <= succ_req.map{|s| [s.key, @ip_port]}
    
    # convert incoming find_req messages into local find_events
    find_event <= find_req.map{|f| [f.key, f.from]}
    
    # if not at successor, forward to closest finger   
    find_req <~ join([find_event, finger, closest, me], [find_event.key, closest.key]).map do |e, f, c, m| 
      stdio <~ [["forward to closest finger!"]]
      [c.succ_addr, e.key, e.from] unless (f.index == 0 and in_range(e.key, m.start, f.succ, true))
    end

    # else at successor, so respond with successor's ID/address
    find_resp <~ join([find_event, finger, me]).map do |e, f, m|
      stdio <~ [["at successor!"]]
      [e.from, e.key, f.succ, f.succ_addr] if f.index == 0 and in_range(e.key, m.start, f.succ, true)
    end
    
    # when we receive a response, put it to the output interface
    succ_resp <= find_resp.map { |f| [f.key, f.start, f.addr] }
  end
end
