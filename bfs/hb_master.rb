require 'rubygems'
require 'backports'
require 'bud'
require 'heartbeat/heartbeat'

module HBMaster
  include HeartbeatAgent

  state do
    #scratch :chunk_cache, [:node, :chunkid]
    table :chunk_cache, [:node, :chunkid, :time]
    scratch :chunk_cache_nodes, [:node]
    # at any given time, :available will contain a list of datanodes in preference order.
    # for now, arbitrary
    scratch :available, [] => [:pref_list]
    periodic :master_duty_cycle, 1
  end

  declare 
  def hblogic
    #stdio <~ last_heartbeat.inspected
    chunk_cache <= join([master_duty_cycle, last_heartbeat]).flat_map do |d, l| 
      unless l.payload == -1
        l.payload.map do |pay|
          #puts "CC: #{l.inspect}" or [l.peer, pay, Time.parse(d.val).to_f]
          [l.peer, pay, Time.parse(d.val).to_f]
        end 
      end
    end

    chunk_cache <- join([master_duty_cycle, chunk_cache]).map do |t, c|
      unless last_heartbeat.map{|h| h.peer}.include? c.node
        #puts "DEL from chunk cache #{c.inspect}" or c
        c
      end
    end

    chunk_cache_nodes <= chunk_cache.map{ |cc| [cc.node] }
    available <= chunk_cache_nodes.group(nil, accum(chunk_cache_nodes.node))
    #stdio <~ available.inspected
  end
end
