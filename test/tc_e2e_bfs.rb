require 'rubygems'
require 'test/unit'
require 'digest/md5'
require 'bfs/fs_master'
require 'bfs/datanode'
require 'bfs/hb_master'
require 'bfs/chunking'
require 'bfs/bfs_master'
require 'bfs/bfs_client'
require 'bfs/background'

TEST_FILE='/usr/share/dict/words'

class DN
  include Bud
  include BFSDatanode
end

class TestBFS < Test::Unit::TestCase
  def initialize(args)
    @opts = {}
    clean
    super
  end

  def clean
    `rm -r #{DATADIR}`
  end

  def md5_of(name)
    Digest::MD5.hexdigest(File.read(name))
  end
  
  def files_in_dir(dir)
    Dir.new(dir).entries.length - 2
  end 

  def ntest_concurrent_clients
    b = BFSMasterServer.new(@opts.merge(:port => 44444))
    d1 = new_datanode(41111, 44444)
    d2 = new_datanode(41112, 44444)
    s1 = BFSShell.new("localhost:44444")
    s2 = BFSShell.new("localhost:44444")

    b.run_bg; s1.run_bg; s2.run_bg


    s1.dispatch_command(["create", "/test1"])
    #res = s2.dispatch_command(["ls", "/"])
    s2.dispatch_command(["create", "/test2"])

    

    Thread.new do 
      rd = File.open(TEST_FILE, "r")
      s1.dispatch_command(["append", "/test1"], rd)
      rd.close
    end

    rd2 = File.open(TEST_FILE, "r")
    s2.dispatch_command(["append", "/test2"], rd2)
    rd2.close

    sleep 6


    file = "/tmp/bfstest_"  + (1 + rand(1000)).to_s
    fp = File.open(file, "w")
    s1.dispatch_command(["read", "/test1"], fp)
    fp.close
    assert_equal(md5_of(TEST_FILE), md5_of(file))


    file = "/tmp/bfstest_"  + (1 + rand(1000)).to_s
    fp = File.open(file, "w")
    s2.dispatch_command(["read", "/test2"], fp)
    fp.close
    assert_equal(md5_of(TEST_FILE), md5_of(file))
    
    
  end
    
  def ntest_many_datanodes
    b = BFSMasterServer.new(@opts.merge(:port => "33333"))
    b.run_bg
    
    dns = []
    ports = []
    (0..6).each do |i|
      port = 31111 + i
      ports << port
      dns << new_datanode(port, 33333)
    end

    s = BFSShell.new("localhost:33333", @opts)
    s.run_bg

    s.dispatch_command(["create", "/peter"])
    s.sync_do{}
    rd = File.open(TEST_FILE, "r")
    s.dispatch_command(["append", "/peter"], rd)
    rd.close

    chunk = {}
    node = {}
    ports.each do |p|
      dir = "/tmp/bloomfs/#{p}"
      #len = files_in_dir(dir)
      Dir.new(dir).each do |entry|
        next if entry =~ /^\./
        unless chunk[entry]
          chunk[entry] = []
        end
        chunk[entry] << p
  
        unless node[p]
          node[p] = []
        end 
        node[p] << entry
      end
    end     
  
    assert_equal(25, chunk.keys.length)
    chunk.each_pair do |k, v|
      assert(v.length >= REP_FACTOR, "low replication: #{v.length} for #{k}")
      if v.length > REP_FACTOR 
        puts "\texpeced #{REP_FACTOR}, got #{v.length} for #{k}"
      end
    end

    node.each_pair do |k, v|
      #puts "nodes[#{k}] = #{v.inspect}"
      assert(v.length > 0, "node #{k} has no chunks")
    end

    dns.each {|d| d.stop_datanode }
    b.stop_bg
    s.stop_bg

  end
  
  def do_read(rt)
    file = "/tmp/bfstest_"  + (1 + rand(1000)).to_s
    fp = File.open(file, "w")
    rt.dispatch_command(["read", "/peter"], fp)
    fp.close
    assert_equal(md5_of(TEST_FILE), md5_of(file)) 
  end

  def test_client
    b = BFSMasterServer.new(@opts.merge(:port => "65433"))
    b.run_bg
    dn = new_datanode(11117, 65433)
    dn2= new_datanode(11118, 65433)

    s = BFSShell.new("localhost:65433")
    s.run_bg

    s.dispatch_command(["mkdir", "/foo"])
    s.dispatch_command(["mkdir", "/bar"])
    s.dispatch_command(["mkdir", "/baz"])
    s.sync_do{}
    s.dispatch_command(["mkdir", "/foo/bam"])
    s.dispatch_command(["create", "/peter"])

    s.sync_do{}
    rd = File.open(TEST_FILE, "r")
    s.dispatch_command(["append", "/peter"], rd)
    rd.close  
    s.sync_do{}
    do_read(s)

    dn.stop_datanode

    # failover
    do_read(s)

    dn2.stop_datanode

    assert_raise(IOError) {do_read(s)}

    # resurrect a datanode and its state
    dn3 = new_datanode(11117, 65433)
    # and an amnesiac
    dn4 = new_datanode(11119, 65433)

    do_read(s)

    # kill the memory node
    dn3.stop_datanode

    # and run off the replica
    do_read(s)

    dn4.stop_datanode
    s.stop_bg
    b.stop_bg
  end
  
  # not the dryest
  def new_datanode(dp, master_port)
    dn = DN.new(dp, @opts.merge(:tag => "P#{dp}"))
    dn.add_member <+ [["localhost:#{master_port}", 1]]
    dn.run_bg
    return dn
  end

  
end

