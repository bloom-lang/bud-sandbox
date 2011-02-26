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

module FSUtil
  include FSProtocol

  state {
    table :remember_resp, fsret.key_cols => fsret.cols
  }

  declare
  def remz
    remember_resp <= fsret
    #rem_av <= available
  end
end

class FSC
  include Bud
  include KVSFS
  include FSUtil
end

class CFSC

  # the completely composed BFS
  include Bud
  include ChunkedKVSFS
  include HBMaster
  include BFSMasterServer
  include BFSBackgroundTasks
  include StaticMembership
  include FSUtil
end


class DN
  include Bud
  include BFSDatanode
end

class HBA
  include Bud
  #include HeartbeatAgent
  include HBMaster
  include StaticMembership
  # PAA
  #include FSUtil
  #include ChunkedKVSFS
  #include BFSMasterServer
end

class TestBFS < Test::Unit::TestCase
  def initialize(args)
    @opts = {}
    `rm -r #{DATADIR}`
    super
  end

  def md5_of(name)
    Digest::MD5.hexdigest(File.read(name))
  end
    
  def test_client
    b = CFSC.new(@opts.merge(:port => "65433"))
    b.run_bg
    dn = new_datanode(11117, 65433)
    dn2= new_datanode(11118, 65433)
    sleep 2


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

    s.dispatch_command(["ls", "/"])
    file = "/tmp/bfstest_"  + (1 + rand(1000)).to_s
    fp = File.open(file, "w")
    s.dispatch_command(["read", "/peter"], fp)
    fp.close
   
    assert_equal(md5_of(TEST_FILE), md5_of(file)) 
    
    #dump_internal_state(b)
    dn.stop_datanode

    # failover
    file = "/tmp/bfstest_"  + (1 + rand(1000)).to_s
    fp = File.open(file, "w")
    s.dispatch_command(["read", "/peter"], fp)
    fp.close
    assert_equal(md5_of(TEST_FILE), md5_of(file)) 

    dn2.stop_datanode


    assert_raise(RuntimeError) {s.dispatch_command(["read", "/peter"], fp)}

    # resurrect a datanode and its state
    dn3 = new_datanode(11117, 65433)
    # and an amnesiac
    dn4 = new_datanode(11119, 65433)
    sleep 3

    file = "/tmp/bfstest_"  + (1 + rand(1000)).to_s
    fp = File.open(file, "w")
    s.dispatch_command(["read", "/peter"], fp)
    fp.close
    assert_equal(md5_of(TEST_FILE), md5_of(file)) 

    # kill the memory node
    dn3.stop_datanode

    # and run off the replica
    
    
    file = "/tmp/bfstest_"  + (1 + rand(1000)).to_s
    fp = File.open(file, "w")
    s.dispatch_command(["read", "/peter"], fp)
    fp.close
    assert_equal(md5_of(TEST_FILE), md5_of(file)) 


    s.stop_bg
    b.stop_bg
  
  end
  
end

