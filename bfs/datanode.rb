require 'rubygems'
require 'bud'
require 'backports'
require 'heartbeat/heartbeat'
require 'membership/membership'
require 'bfs/data_protocol'

module BFSDatanode
  include HeartbeatAgent
  include StaticMembership

  state {
    table :local_chunks, [:chunkid, :size]
    table :data_port, [] => [:port]
    scratch :chunk_summary, [:payload]
    #periodic :dirscan_timer, 3 
  }

  bootstrap do
    # fake; we'd read these from the fs
    # in the original bfs, we actually polled a directory, b/c
    # the chunks were written by an external process.
    chunk_summary <+ [[-1, -1]]
    #super
  end

  declare 
  def hblogic
    chunk_summary <= hb_timer.map do |t|
      [Dir.new(DATADIR).to_a.map{|d| d.to_i unless d =~ /\./}] if chunk_summary.empty?
    end
    #payload <= chunk_summary.group(nil, accum(chunk_summary.payload))
    payload <= chunk_summary#.group(nil, accum(chunk_summary.payload))

  end

  def initialize(dataport, opts)
    super(opts)
    @dp_server = DataProtocolServer.new(dataport)
    # fix!
    # I should be able to do this in bootstrap, but it appears racy 
    return_address <+ [["localhost:#{dataport}"]]
  end

  def stop_datanode
    # unsafe, unsage
    @dp_server.stop_server
    stop_bg
  end
end
