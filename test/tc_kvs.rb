require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/kvs_workloads'
require 'kvs/kvs'
require 'kvs/useful_combos'

class TestKVS < Test::Unit::TestCase
  include KVSWorkloads

  def initialize(args)
    #@opts = {:dump => true, :visualize => 3, :scoping => false}
    @opts = {}
    super
  end

  def test_wl2
    # reliable delivery fails if the recipient is down
    v = SingleSiteKVS.new#(:visualize => 3)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    sleep 1
    if v.is_a?  ReliableDelivery
      sleep 1
      workload1(v)
      assert_equal(0, v.kvstate.length)
    end
  end

  def ntest_wl5
    # (temporarily?) disabled.
    # the unmetered kvs should fail on a ``disorderly'' workload
    # however, async_do forces a FIFO, coincidence-free delivery order
    v = SingleSiteKVS.new(@opts.merge(:port => 12352))
    assert_nothing_raised(RuntimeError) {v.run_bg}
    #add_members(v, "localhost:12352")
    assert_raise(Bud::KeyConstraintError)  { workload2(v) }
    v.stop_bg
  end

  def test_wl1
    # in a distributed, ordered workload, the right thing happens
    v = BestEffortReplicatedKVS.new(@opts.merge(:tag => 'dist_primary', :port => 12345))
    v2 = BestEffortReplicatedKVS.new(@opts.merge(:tag => 'dist_backup', :port => 12346))
    add_members(v, "localhost:12345", "localhost:12346")
    add_members(v2, "localhost:12345", "localhost:12346")

    assert_nothing_raised(RuntimeError) {v.run_bg}
    assert_nothing_raised(RuntimeError) {v2.run_bg}
    workload1(v)
    v.sync_do{ assert_equal(1, v.kvstate.length) }
    v.sync_do{ assert_equal("bak", v.kvstate.first[1]) } 
    v2.sync_do{ assert_equal(1, v2.kvstate.length) }
    v2.sync_do{ assert_equal("bak", v2.kvstate.first[1]) }
    v.stop_bg
    v2.stop_bg
  end

  def test_simple
    v = SingleSiteKVS.new(:port => 12360, :tag => 'simple')
    assert_nothing_raised(RuntimeError) {v.run_bg}
    workload1(v)
    v.sync_do{ assert_equal(1, v.kvstate.length) }
    v.sync_do{ assert_equal("bak", v.kvstate.first[1]) }

    v.sync_do{ v.kvget <+ [[1234, 'foo']] }
    s.sync_do{ 
      assert_equal(1, v.kvget_response.length)
      assert_equal("bak", v.kvget_response.first[1])
    }

    v.stop_bg
  end
end

