require 'rubygems'
require 'test/unit'
require 'bfs/fs_master'

class FSC < Bud
  include KVSFS
end

class TestBFS < Test::Unit::TestCase
  def test_fsmaster
    b = FSC.new(:visualize => 3)
    b.run_bg

    b.sync_do{ b.fscreate <+ [[3425, 'foo', '/']] } 
    b.sync_do{ assert(b.fsret.first.status) }
    b.sync_do{ b.fsls <+ [[123, '/']] }
    b.sync_do{ 

      assert(b.fsret.first.status)
      assert_equal(1, b.fsret.length) 
      assert_equal(["foo"], b.fsret.first.data)
    } 

    b.sync_do{ b.fscreate <+ [[3425, 'bar', '/']] } 
    b.sync_do{ assert(b.fsret.first.status) }
    b.sync_do{ b.fsls <+ [[124, '/']] }
    b.sync_do {
      assert_equal(1, b.fsret.length)
      assert_equal(["foo", "bar"], b.fsret.first.data)
    }

    b.sync_do{ b.fsmkdir <+ [[234, 'sub1', '/']] }
    b.sync_do{ assert(b.fsret.first.status) }
    b.sync_do{ b.fsmkdir <+ [[235, 'sub2', '/']] }
    b.sync_do{ assert(b.fsret.first.status) }
    
    b.sync_do { b.kvstate.each{|k| puts "KVS: #{k.inspect}" } }

    b.sync_do{ b.fsmkdir <+ [[236, 'subsub1', '/sub1']] }
    b.sync_do{ assert(b.fsret.first.status) }
    b.sync_do{ b.fsmkdir <+ [[237, 'subsub2', '/sub2']] }
    b.sync_do{ assert(b.fsret.first.status) }

    b.sync_do{ b.fsls <+ [[125, '/']] }
    b.sync_do {
      b.fsret.each{|r| puts "RET: #{r.inspect}" } 
      assert_equal(1, b.fsret.length)
      assert_equal(["foo", "bar", "sub1", "sub2"], b.fsret.first.data)
    }
    b.sync_do{ b.fsls <+ [[125, '/sub1']] }
    b.sync_do {
      b.fsret.each{|r| puts "RET: #{r.inspect}" } 
      assert_equal(1, b.fsret.length)
      assert_equal(["subsub1"], b.fsret.first.data)
    }


  end
end

