require 'rubygems'
require 'backports'
require 'bud'
require 'heartbeat/heartbeat'

module HBMaster
  include HeartbeatAgent
  #include HeartbeatProtocol
  #include StaticMembership

  state {
    table :chunks, [:node, :file, :chunkid]
    scratch :chunk_summary, [:payload]
  }

  declare 
  def hblogic
    chunks <= last_heartbeat.flat_map do |l| 
      #l.payload.map{ |p|  [l.peer, p[0], p[1]] } 
      l.payload.map{ |p|  p.clone.unshift(l.peer) } 
    end
  end
end
