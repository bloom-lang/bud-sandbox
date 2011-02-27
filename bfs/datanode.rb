require 'rubygems'
require 'bud'
require 'backports'
require 'heartbeat/heartbeat'
require 'membership/membership'
require 'bfs/data_protocol'

module BFSDatanode
  include HeartbeatAgent
  include StaticMembership

  state do
    table :local_chunks, [:chunkid, :size]
    table :data_port, [] => [:port]
    #scratch :chunk_summary, [:payload]
  end

  declare 
  def hblogic
    #chunk_summary <= hb_timer.map do |t|
    payload <= hb_timer.map do |t|
      [Dir.new("#{DATADIR}/#{@data_port}").to_a.map{|d| d.to_i unless d =~ /\./}] #if chunk_summary.empty?
    end
    #payload <= chunk_summary

  end

  def initialize(dataport, opts)
    super(opts)
    @data_port = dataport
    @dp_server = DataProtocolServer.new(dataport)
    return_address <+ [["localhost:#{dataport}"]]
  end

  def stop_datanode
    # unsafe, unsage
    @dp_server.stop_server
    stop_bg
  end
end
