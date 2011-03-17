require 'rubygems'
require 'bud'
require 'backports'
require 'heartbeat/heartbeat'
require 'membership/membership'
require 'bfs/data_protocol'
require 'bfs/bfs_client_proto'

module BFSDatanode
  include HeartbeatAgent
  include StaticMembership
  include TimestepNonce
  include BFSHBProtocol

  state do
    scratch :dir_contents, [:file, :time]
    table :last_dir_contents, [:nonce, :file, :time]
    scratch :to_payload, [:nonce, :file, :time]
    scratch :payload_buff, [:nonce, :payload]
    table :server_knows, [:file]
  end

  declare 
  def hblogic
    dir_contents <= hb_timer.flat_map do |t|
      dir = Dir.new("#{DATADIR}/#{@data_port}")
      files = dir.to_a.map{|d| d.to_i unless d =~ /^\./}.uniq!
      dir.close
      files.map {|f| [f, Time.parse(t.val).to_f]}
    end

    to_payload <= join([dir_contents, nonce]).map do |c, n|
      unless server_knows.map{|s| s.file}.include? c.file
        [n.ident, c.file, c.time]
      end
    end
    # base case
    to_payload <= nonce.map {|n| [n.ident, nil, -1]}
    # remember the stuff we cast
    last_dir_contents <+ to_payload
    # if we get an ack, permanently remember
    acked_contents = join([hb_ack, last_dir_contents], [hb_ack.val, last_dir_contents.nonce])
    server_knows <= acked_contents.map {|a, c| [c.file]}
    # and clean up the cache
    last_dir_contents <- acked_contents.map{|a, c| c}
    # turn a set into an array
    payload_buff <= to_payload.group([to_payload.nonce], accum(to_payload.file))
    payload <= payload_buff.map {|b| [[b.nonce, b.payload]]}
  end

  def initialize(dataport=nil, opts={})
    super(opts)
    @data_port = dataport.nil? ? 0 : dataport
    @dp_server = DataProtocolServer.new(dataport)
    return_address <+ [["localhost:#{dataport}"]]
  end

  def stop_datanode
    @dp_server.stop_server
    stop_bg
  end
end
