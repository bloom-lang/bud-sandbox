require 'rubygems'
require 'test/unit'
require 'bfs/fs_master'
require 'bfs/datanode'
require 'bfs/hb_master'
require 'bfs/chunking'

module FSUtil
  include FSProtocol

  state {
    table :remember_resp, fsret.key_cols => fsret.cols
  }

  declare
  def remz
    remember_resp <= fsret
  end
end

class FSC
  include Bud
  include KVSFS
  include FSUtil
end

class CFSC
  include Bud
  include ChunkedKVSFS
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
end

class TestBFS < Test::Unit::TestCase
  def ntest_fsmaster
    b = FSC.new(:dump => true)
    b.run_bg
    do_basic_fs_tests(b)
  end

  def test_chunked_fsmaster
    dn = DN.new
    dn.add_member <+ [["localhost:65432"]]
    dn.run_bg

    dn2 = DN.new
    dn2.add_member <+ [["localhost:65432"]]
    dn2.run_bg

    b = CFSC.new(:port => 65432, :dump => true)
    b.run_bg
    sleep 5
    do_basic_fs_tests(b)
    b.sync_do {  b.fschunklocations <+ [[654, 1, 1]] }
    sleep 1
    b.sync_do { 
      b.chunks.each{|c| puts "CHUNK: #{c.inspect}" } 
      b.remember_resp.each do |r| 
        puts "CHYBKRET: #{r.inspect}" 
        if r.reqid == 654
          assert_equal(2, r.data.length)
        end
      end
    }
    b.stop_bg
    dn.stop_bg
    dn2.stop_bg
  end

  def assert_resp(inst, reqid, data)
    inst.sync_do {
      inst.remember_resp.each do |r|
        if r.reqid == reqid
          assert(r.status)
          assert_equal(data, r.data)
        end
      end
    }
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

  def ntest_datanode
    dn = DN.new(:dump => true)
    dn.add_member <+ [['localhost:45637']]

    hbc = HBA.new(:port => 45637, :dump => true)
    dn.run_bg
    hbc.run_bg
    dn.sync_do  {
      dn.payload.each{|p| puts "PL: #{p.inspect}" }
      dn.member.each{|m| puts "DNM: #{m.inspect}" } 
    }
      
    sleep 3

    hbc.sync_do {
      hbc.last_heartbeat.each{|l| puts "LHB: #{l.inspect}" }
      hbc.chunks.each{|l| puts "CH: #{l.inspect}" }
    }

    hbc.stop_bg
    dn.stop_bg
  end
end

