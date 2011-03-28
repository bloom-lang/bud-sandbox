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
#TEST_FILE="HBA_rewritten.txt"

module FSUtil
  include FSProtocol

  state do
    table :remember_resp, fsret.key_cols => fsret.val_cols
  end

  bloom :remz do
    remember_resp <= fsret
  end
end

class FSC
  include Bud
  # PAA
  include ChunkedKVSFS
  include KVSFS
  include FSUtil
end

class DN
  include Bud
  include BFSDatanode
end

class HBA
  include Bud
  include HBMaster
  include StaticMembership
end

class TestBFS < Test::Unit::TestCase
  def initialize(args)
    @opts = {}
    `rm -r #{DATADIR}`
    super
  end
  def test_directorystuff1
    b = BFSMasterServer.new(@opts.merge(:port => "65432", :trace => true))
    b.run_bg
    s = BFSShell.new("localhost:65432")
    s.run_bg

    s.dispatch_command(["mkdir", "/foo"])
    s.dispatch_command(["mkdir", "/bar"])
    s.dispatch_command(["mkdir", "/baz"])
    s.dispatch_command(["mkdir", "/foo/1"])
    s.dispatch_command(["mkdir", "/foo/1/2"])
    s.dispatch_command(["mkdir", "/foo/1/2/3"])
    s.dispatch_command(["create", "/bar/f1"])
    s.dispatch_command(["create", "/bar/f2"])
    s.dispatch_command(["create", "/bar/f3"])
    s.dispatch_command(["create", "/bar/f4"])
    s.dispatch_command(["create", "/bar/f5"])
    s.dispatch_command(["create", "/foo/1/2/3/nugget"])
    one = s.dispatch_command(["ls", "/"])
    assert_equal(["foo", "bar", "baz"], one)
    two = s.dispatch_command(["ls", "/foo"])
    assert_equal(["1"], two)
    three = s.dispatch_command(["ls", "/bar"])
    assert_equal(["f1", "f2", "f3", "f4", "f5"], three)
    four = s.dispatch_command(["ls", "/foo/1/2"])
    assert_equal(["3"], four)

    #dump_internal_state(b)
    b.stop_bg
    s.stop_bg
  end

  def dump_internal_state(rt)
    rt.sync_do {
      rt.remember_resp.each do |r|
        puts "REM: #{r.inspect}"
      end
      
      rt.kvstate.each{ |k| puts "kvstate: #{k.inspect}" }
    }
  end

  def test_fsmaster
    b = FSC.new(@opts.merge(:trace => true))
    b.run_bg
    do_basic_fs_tests(b)
    b.stop_bg
  end

  def test_rms
    m = BFSMasterServer.new(@opts.merge(:port => 46363))
    m.run_bg
    b = BFSShell.new("localhost:46363")
    b.run_bg
    
    b.dispatch_command(['mkdir', '/a'])
    b.dispatch_command(['mkdir', '/a/b'])
    b.dispatch_command(['mkdir', '/a/b/c'])
    b.dispatch_command(['mkdir', '/a/b/c/d'])
    ret = b.dispatch_command(['rm', '/a'])
    assert(!ret[1], "rm of non-empty directory should fail")

    ret = b.dispatch_command(['rm', '/a/b'])
    assert(!ret[1], "rm of non-empty directory should fail")
    
    ret = b.dispatch_command(['rm', '/a/b/c/d'])
    assert(ret[1], "rm of empty directory should succeed")

    ret = b.dispatch_command(['rm', '/a/b/c'])
    assert(ret[1], "rm of empty directory should succeed")
  
    ret = b.dispatch_command(['rm', '/a/b'])
    assert(ret[1], "rm of empty directory should succeed")

    ret = b.dispatch_command(['ls', '/'])

    m.sync_do
    m.sync_do
    m.sync_do
    m.sync_do
    m.sync_do do 
      m.kvstate.each do |k|
        puts "STATE: #{k.inspect}"
      end
    end   

    m.stop_bg
    b.stop_bg
  end
  
  def new_datanode(dp, master_port)
    dn = DN.new(dp, @opts)
    dn.add_member <+ [["localhost:#{master_port}", 1]]
    dn.run_bg
    return dn
  end

  def tnest_addchunks
    dn = new_datanode(11112, 65432)
    #dn2 = new_datanode(11113, 65432)

    b = BFSMasterServer.new(@opts.merge(:port => "65432"))
    b.run_bg
    do_basic_fs_tests(b)
    do_addchunks(b)

    dn.stop_datanode
    #dn2.stop_bg
    b.stop_bg
  end

  def assert_resp(inst, reqid, data)
    inst.sync_do {
      inst.remember_resp.each do |r|
        if r.reqid == reqid
          assert(r.status, "call #{reqid} should have succeeded with #{data}.  Instead: #{r.inspect}")
          assert_equal(data, r.data)
        end
      end
    }
  end

  def do_addchunks(b)
    c1 = addchunk(b, "/foo", 5678)
    c2 = addchunk(b, "/foo", 6789)
    c3 = addchunk(b, "/foo", 67891)
    c4 = addchunk(b, "/foo", 67892)
  end

  def addchunk(b, name, id)
    b.sync_do{ b.fsaddchunk <+ [[id, name]] }
   
    chunkid = nil
    b.sync_do {
      b.remember_resp.each do |r|
        if r.reqid == id
          chunkid = r.data
        end
      end
    }
    return chunkid
  end     

  def do_basic_fs_tests(b)
    b.sync_do{ b.fscreate <+ [[3425, 'foo', '/']] } 

    assert_resp(b, 3425, nil)
    b.sync_do{ b.fsls <+ [[123, '/']] }
    assert_resp(b, 123, ["foo"])
    b.sync_do{ b.fscreate <+ [[3426, 'bar', '/']] } 
    assert_resp(b, 3426, nil)
    b.sync_do{ b.fsls <+ [[124, '/']] }
    assert_resp(b, 124, ["foo", "bar"])
    b.sync_do{ b.fsmkdir <+ [[234, 'sub1', '/']] }
    assert_resp(b, 234, nil)
    b.sync_do{ b.fsmkdir <+ [[235, 'sub2', '/']] }
    assert_resp(b, 235, nil)
    b.sync_do{ b.fsmkdir <+ [[236, 'subsub1', '/sub1']] }
    assert_resp(b, 236, nil)
    b.sync_do{ b.fsmkdir <+ [[237, 'subsub2', '/sub2']] }
    assert_resp(b, 237, nil)
    b.sync_do{ b.fsls <+ [[125, '/']] }
    assert_resp(b, 125, ["foo", "bar", "sub1", "sub2"])
    b.sync_do{ b.fsls <+ [[126, '/sub1']] }
    assert_resp(b, 126, ["subsub1"])
  end
end

