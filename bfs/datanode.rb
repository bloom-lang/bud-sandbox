require 'rubygems'
require 'bud'
require 'backports'
require 'heartbeat/heartbeat'
require 'membership/membership'

DATADIR = "/tmp/bloomfs"
CHUNKSIZE = 100000

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
    local_chunks <+ [[-1, -1]]
    #super
  end

  declare 
  def hblogic
    chunk_summary <= hb_timer.map do |t|
      [Dir.new(DATADIR).to_a.map{|d| d.to_i unless d =~ /\./}] if chunk_summary.empty?
    end
    

    
    #chunk_summary <= local_chunks.map{|c| [[c.file, c.ident]] } 
    payload <= chunk_summary.group(nil, accum(chunk_summary.payload))
    #payload <= local_chunks.group(nil, accum(local_chunks.chunkid))

  end

  def initialize(dataport, opts)
    super(opts)
    @data_port = dataport
    puts "DATAPORT: #{@data_port}"
    Dir.mkdir(DATADIR) unless File.directory? DATADIR
    # fix!
    # I should be able to do this in bootstrap, but it appears racy 
    return_address <+ [["localhost:#{@data_port}"]]
    #return_address <= [["localhost:#{@data_port}"]]
    start_datanode_server(dataport)
  end

  def start_datanode_server(port)
    Thread.new do     
      @dn_server = TCPServer.open(port)
      loop {
        client = @dn_server.accept
        puts "GOT A CLIENT: #{client.inspect}"
        Thread.new do 
  
          header = client.gets
          puts "got header #{header}"
          # header should contain a chunk id followed by a list of candidate datanodes
          # already sorted in preference order.
          elems = header.split(",")
          chunkid = elems.shift
          nextnode = elems.shift
          puts "chunkid is #{chunkid}"
          chunkfile = File.open("#{DATADIR}/#{chunkid.to_i.to_s}", "w")
          data = client.read(CHUNKSIZE)
          #puts "write data #{data}"
          chunkfile.write data
          chunkfile.close
          client.close
  
          # (thread)  
        end
      }
    end
  end

  def stop_datanode
    # unsafe, unsage
    @dn_server.close
    stop_bg
  end
end
