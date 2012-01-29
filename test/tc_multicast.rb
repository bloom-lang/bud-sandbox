require 'rubygems'
require 'bud'
require 'test/unit'
require 'delivery/multicast'

module TestState
  include StaticMembership

  state do
    table :mcast_done_perm, [:ident]
    table :rcv_perm, [:ident] => [:payload]
  end

  bloom :mem do
    mcast_done_perm <= mcast_done
    rcv_perm <= pipe_out {|r| [r.ident, r.payload]}
  end
end

class MC
  include Bud
  include TestState
  include BestEffortMulticast

end

class RMC
  include Bud
  include TestState
  include ReliableMulticast
end


class TestMC < Test::Unit::TestCase
  def test_be
    mc = MC.new
    mc2 = MC.new
    mc3 = MC.new

    mc2.run_bg; mc3.run_bg
    mc.add_member <+ [[1, mc2.ip_port], [2, mc3.ip_port]]
    mc.run_bg

    mc.sync_do{ mc.mcast_send <+ [[1, 'foobar']] }

    mc.tick

    mc.sync_do{ assert_equal(1, mc.mcast_done_perm.length) }
    
    mc.sync_do{ assert_equal(1, mc.mcast_done_perm.first.ident) }
    mc.sync_do{ assert_equal(1, mc2.rcv_perm.length) }
    mc.sync_do{ assert_equal(1, mc3.rcv_perm.length) }
    mc.sync_do{ assert_equal(1, mc2.rcv_perm.first.ident) }

    mc.stop
    mc2.stop
    mc3.stop
  end

  def ntest_reliable
    mc = RMC.new
    mc2 = RMC.new
    mc3 = RMC.new

    mc2.run_bg; mc3.run_bg
    mc.add_member <+ [[1, mc2.ip_port], [2, mc3.ip_port]]
    mc.run_bg

    resps = mc.sync_callback(mc.mcast_send.tabname, [[1, 'foobar']], mc.mcast_done.tabname)
    assert_equal([[1, 'foobar']], resps.to_a.sort)

    assert_equal(mc2.rcv_perm.length, 1)
    assert_equal(mc3.rcv_perm.length, 1)
    assert(mc2.rcv_perm.include? [1, 'foobar'])
    assert(mc3.rcv_perm.include? [1, 'foobar'])

    mc.stop
    mc2.stop
    mc3.stop
  end
end
