require 'rubygems'
require 'backports'
require 'bud'
require 'heartbeat/heartbeat'

module HBMaster
  include Anise
  include HeartbeatAgent
  #include HeartbeatProtocol
  #include StaticMembership
  annotator :declare

  def state
    super
    table :chunks, [:node, :file, :chunkid]
    scratch :chunk_summary, [:payload]
  end

  declare 
  def hblogic
    chunks <= last_heartbeat.flat_map do |l| 
      #l.payload.map{ |p|  [l.peer, p[0], p[1]] } 
      l.payload.map{ |p|  p.clone.unshift(l.peer) } 
    end
  end

  
end
