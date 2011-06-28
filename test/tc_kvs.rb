require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/kvs_workloads'
require 'kvs/kvs'
require 'kvs/useful_combos'
require 'ordering/serializer'
require 'ordering/nonce'

class TestKVS < Test::Unit::TestCase
  include KVSWorkloads

  def initialize(args)
    @opts = {}
    super
  end

  def test_wl2
    # reliable delivery fails if the recipient is down
    v = SingleSiteKVS.new
    v.run_bg
    if v.is_a? ReliableDelivery
      workload1(v)
      assert_equal(0, v.kvstate.length)
    end
  end

  def ntest_wl5
    # (temporarily?) disabled.
    # the unmetered kvs should fail on a ``disorderly'' workload
    # however, async_do forces a FIFO, coincidence-free delivery order
    v = SingleSiteKVS.new(@opts.merge(:port => 12352))
    v.run_bg
    #add_members(v, "localhost:12352")
    assert_raise(Bud::KeyConstraintError)  { workload2(v) }
    v.stop_bg
  end

  def test_wl1
    # in a distributed, ordered workload, the right thing happens
    v = BestEffortReplicatedKVS.new(@opts.merge(:tag => 'dist_primary', :port => 12345, :dump_rewrite => true, :trace => true))
    v2 = BestEffortReplicatedKVS.new(@opts.merge(:tag => 'dist_backup', :port => 12346, :trace => true))
    add_members(v, v.ip_port, v2.ip_port)
    add_members(v2, v.ip_port, v2.ip_port)

    v.run_bg
    v2.run_bg

    workload1(v)
    # what are we going to do about name-mangling in the module system?
    v.sync_do{ assert_equal(1, v.kvs__kvstate.length) }
    v.sync_do{ assert_equal("bak", v.kvs__kvstate.first[1]) }
    v2.sync_do{ assert_equal(1, v2.kvs__kvstate.length) }
    v2.sync_do{ assert_equal("bak", v2.kvs__kvstate.first[1]) }
    v.stop_bg
    v2.stop_bg
  end

  def test_simple
    v = SingleSiteKVS.new(:tag => 'simple', :port => 12345)
    v.run_bg
    workload1(v)
    v.sync_do { assert_equal(1, v.kvstate.length) }
    v.sync_do { assert_equal("bak", v.kvstate.first[1]) }

    v.sync_do { v.kvget <+ [[1234, 'foo']] }
    v.sync_do {
      assert_equal(1, v.kvget_response.length)
      assert_equal("bak", v.kvget_response.first[2])
    }

    v.stop_bg
  end

  def test_del
    v = SingleSiteKVS.new(:tag => 'simple')
    v.run_bg
    workload1(v)
    v.sync_do { assert_equal(1, v.kvstate.length) }
    v.sync_do { assert_equal("bak", v.kvstate.first[1]) }

    v.sync_do { v.kvdel <+ [['foo', 23525]] }
    v.sync_do
    v.sync_do { assert_equal(0, v.kvstate.length) }
    v.stop_bg
  end

  def ntest_persistent_kvs
    dir = "/tmp/tpk"
    `rm -r #{dir}`
    `mkdir #{dir}`
    p  = SSPKVS.new(:dbm_dir => dir)
    p.run_bg
    workload1(p)
    p.sync_do { assert_equal(1, p.kvstate.length) }
    p.sync_do { assert_equal("bak", p.kvstate.first[1]) }
    p.stop_bg
    
    p2 = SSPKVS.new(:dbm_dir => dir)
    p2.run_bg
    p2.sync_do{}
    p2.sync_do{}
    p2.sync_do{}
    p2.sync_do { assert_equal(1, p2.kvstate.length) }
    p2.sync_do { assert_equal("bak", p2.kvstate.first[1]) }
    p2.stop_bg
  end   

  class XK
    include Bud
    include TwoPLTransactionalKVS
  end

  def test_xact_kvs
    p = XK.new
    p.run_bg

    p.sync_callback(:xput, [[1, "foo", "bar"]], :xput_response)
    p.sync_callback(:xput, [[1, "bam", "biz"]], :xput_response)
    p.sync_callback(:xput, [[1, "foo", "big"]], :xput_response)
    res = p.sync_callback(:xget, [[1, "foo"]], :xget_response)

    p.register_callback(:xget_response) do |cb|
      cb.each do |row|
        assert_equal(1, row.xid)
        assert_equal("big", row.value)
      end 

    end

    p.sync_do { p.xget <+ [[2, "foo"]] }

    p.sync_do { p.end_xact <+ [[1]] }


    p.register_callback(:xget_response) do |cb|
      cb.each do |row|
        assert_equal(2, row.xid)
        assert_equal("big", row.value)
      end 
    end

  end
end
