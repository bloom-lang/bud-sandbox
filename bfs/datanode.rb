require 'rubygems'
require 'bud'
require 'bfs/bfs_client_proto'
require 'bfs/data_protocol'
require 'heartbeat/heartbeat'
require 'membership/membership'
require 'ordering/nonce'

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

  bloom :hblogic do
    dir_contents <= hb_timer.flat_map do |t|
      dir = Dir.new("#{DATADIR}/#{@data_port}")
      files = dir.to_a.map{|d| d.to_i unless d =~ /^\./}.uniq!
      dir.close
      files.map {|f| [f, t.val.to_f]}
    end

    to_payload <= (dir_contents * nonce).pairs do |c, n|
      unless server_knows.map{|s| s.file}.include? c.file
        #puts "BCAST #{c.file}; server doesn't know" or [n.ident, c.file, c.time]
        [n.ident, c.file, c.time]
      else
        #puts "server knows about #{server_knows.length} files"
      end
    end

    #stdio <~ hb_timer {["DB: #{@data_port}: payload #{to_payload.length}"] if to_payload.length > 2}

    # base case
    to_payload <= nonce {|n| [n.ident, nil, -1]}
    # remember the stuff we cast
    last_dir_contents <+ to_payload
    # if we get an ack, permanently remember
    temp :acked_contents <= (hb_ack * last_dir_contents).pairs(:val => :nonce)
    server_knows <= acked_contents {|a, c| [c.file]}
    # and clean up the cache
    last_dir_contents <- acked_contents {|a, c| c}
    # turn a set into an array
    payload_buff <= to_payload.group([to_payload.nonce], accum(to_payload.file))
    payload <= payload_buff {|b| [[b.nonce, b.payload]]}
  end

  def initialize(dataport=nil, opts={})
    super(opts)
    @data_port = dataport.nil? ? 0 : dataport
    @dp_server = DataProtocolServer.new(dataport)
    return_address <+ [["localhost:#{dataport}"]]
  end

  def stop_datanode
    @dp_server.stop_server
    stop
  end
end
