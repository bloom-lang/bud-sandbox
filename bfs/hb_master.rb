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
    #table :available, [] => [:pref_list]
    scratch :available, [] => [:pref_list]

    scratch :pl, [:pl]
  }

  declare 
  def hblogic
    #stdio <~ last_heartbeat.map{|l| ["lhb: #{l.inspect}"] }
    chunk_cache <= last_heartbeat.flat_map do |l| 
      unless l.payload == -1
      l.payload.map do |pay|
        #puts "unfold PAY #{pay.inspect} (#{pay.class}) vs. #{l.payload.inspect} (#{l.payload.class})" or [l.peer, pay]
        [l.peer, pay]
      end 
      end
    end
  
    chunk_cache_nodes <= chunk_cache.map{ |cc| [cc.node] }
    available <= chunk_cache_nodes.group(nil, accum(chunk_cache.node))
    stdio <~ available.map{ |a| ["avail: #{a.inspect}"] } 
  end
end
