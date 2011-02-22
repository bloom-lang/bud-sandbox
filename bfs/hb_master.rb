require 'rubygems'
require 'backports'
require 'bud'
require 'heartbeat/heartbeat'

module HBMaster
  include HeartbeatAgent

  state {
    table :chunk_cache, [:node, :chunkid]
    #scratch :chunk_cache_nodes, [:node]
    table :chunk_cache_nodes, [:node]

    # at any given time, :available will contain a list of datanodes in preference order.
    # for now, arbitrary
    table :available, [] => [:pref_list]
  }

  declare 
  def hblogic
    #stdio <~ heartbeat
    chunk_cache <= last_heartbeat.flat_map do |l| 
      #l.payload.map{ |p|  [l.peer, p[0]] } 
      #puts "ah, l is #{l.inspect} class #{p.class}"
      #puts "PL" or l.payload.map{ |p| p.clone.unshift(l.peer) } 
      l.payload.map do |pay|
        [l.peer, pay]
      end 
    end
  
    chunk_cache_nodes <= chunk_cache.map{ |cc| [cc.node] }
    available <= chunk_cache_nodes.group(nil, accum(chunk_cache.node))
    stdio <~ available.map{ |a| ["avail: #{a.inspect}"] } 
  end
end
