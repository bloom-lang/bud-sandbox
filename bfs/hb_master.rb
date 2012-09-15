require 'rubygems'
require 'bud'
require 'bfs/bfs_client_proto'
require 'heartbeat/heartbeat'

module HBMaster
  include HeartbeatAgent
  include BFSHBProtocol

  state do
    interface output, :available, [] => [:pref_list]
    table :chunk_cache, [:node, :chunkid] => [:time]
    table :chunk_cache_alive, [:node, :chunkid, :time]
    scratch :chunk_cache_nodes, [:node]
    # at any given time, :available will contain a list of datanodes in preference order.
    # for now, the order is arbitrary
    periodic :master_duty_cycle, MASTER_DUTY_CYCLE
  end

  bloom :hbmasterlogic do
    chunk_cache <+ (master_duty_cycle * last_heartbeat).flat_map do |d, l| 
      l.payload[1].map do |pay|
        unless chunk_cache{|c| c.chunkid if c.node == l.peer}.include? pay
          [l.peer, pay, d.val.to_f] unless pay.nil?
        end
      end 
    end

    chunk_cache_alive <+ (master_duty_cycle * chunk_cache * last_heartbeat).combos(chunk_cache.node => last_heartbeat.peer) do |l, c, h|
      c if (l.val.to_f - h.time) < 3
    end
    
    chunk_cache_alive <- (master_duty_cycle * chunk_cache_alive).rights
    hb_ack <~ heartbeat do |l|
      [l.sender, l.payload[0]] unless l.payload[1] == [nil]
    end
    available <= last_heartbeat.group(nil, accum(last_heartbeat.peer))
  end
end
