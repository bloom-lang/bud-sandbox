require 'rubygems'
require 'bud'
require 'heartbeat/heartbeat'
require 'membership/membership'

DATADIR = "/tmp/bloomfs"
CHUNKSIZE = 100000

module BFSDatanode
  include HeartbeatAgent
  include StaticMembership

  state {
    table :local_chunks, [:file, :ident, :size]
    table :data_port, [] => [:port]
    scratch :chunk_summary, [:payload]
    #periodic :dirscan_timer, 3 
  }

  bootstrap do
    # fake; we'd read these from the fs
    # in the original bfs, we actually polled a directory, b/c
    # the chunks were written by an external process.
    #local_chunks <+ [[1, 1, 1], [1, 2, 1], [1, 3, 1]]
    # fix!!
    puts "BOOT DN: #{@data_port}"
    #return_address <+ [["localhost:#{@data_port}"]]
  end

  declare 
  def hblogic
    #canijoin = [dirscan_timer, Dir.new(DATADIR)]
    #local_chunks <= canijoin.map do |t, d|
    #  [-1, d, 1]
    #end

    local_chunks <= hb_timer.map do
      Dir.new(DATADIR).map do |d|
        #unless d =~ /^\./
          #puts "do chunk with #{d}" or [-1, d.to_i, 1] 
        #end
      end
    end
    
    chunk_summary <= local_chunks.map{|c| [[c.file, c.ident]] } 
    payload <= chunk_summary.group(nil, accum(chunk_summary.payload))

  end

  def initialize(dataport, opts)
    super(opts)
    @data_port = dataport
    puts "DATAPORT: #{@data_port}"
    Dir.mkdir(DATADIR) unless File.directory? DATADIR
    # fix!
    # I should be able to do this in bootstrap, but it appears racy 
    return_address <+ [["localhost:#{@data_port}"]]
    start_datanode_server(dataport)
  end

  def start_datanode_server(port)
    Thread.new do     
      server = TCPServer.open(port)
      loop {
        client = server.accept
        puts "GOT A CLIENT: #{client.inspect}"
        Thread.new do 
  
          header = client.gets
          puts "got header #{header}"
          # header should contain a chunk id followed by a list of candidate datanodes
          # already sorted in preference order.
          elems = header.split(",")
          chunkid = elems.shift
          nextnode = elems.shift
          chunkfile = File.open("#{DATADIR}/#{chunkid}", "w")
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
end
