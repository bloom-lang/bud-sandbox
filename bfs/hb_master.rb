require 'rubygems'
require 'backports'
require 'bud'
require 'heartbeat/heartbeat'
require 'bfs/bfs_client_proto'

module HBMaster
  include HeartbeatAgent
  include BFSHBProtocol

  state do
    interface output, :available, [] => [:pref_list]
    #table :chunk_cache, [:node, :chunkid, :time]
    table :chunk_cache, [:node, :chunkid] => [:time]
    scratch :chunk_cache_alive, [:node, :chunkid, :time]

    scratch :chunk_cache_nodes, [:node]
    # at any given time, :available will contain a list of datanodes in preference order.
    # for now, arbitrary
    periodic :master_duty_cycle, MASTER_DUTY_CYCLE
  end

  bloom :hbmasterlogic do
    #stdio <~ last_heartbeat.inspected
    chunk_cache <+ (master_duty_cycle * last_heartbeat).flat_map do |d, l| 
      l.payload[1].map do |pay|
        unless chunk_cache{|c| c.chunkid}.include? pay
          [l.peer, pay, Time.parse(d.val).to_f] unless pay.nil?
        end
      end 
    end

    chunk_cache_alive <= (chunk_cache * last_heartbeat).lefts

    hb_ack <~ last_heartbeat do |l|
      [l.sender, l.payload[0]] unless l.payload[1] == [nil]
    end

    available <= last_heartbeat.group(nil, accum(last_heartbeat.peer))
  end
end
