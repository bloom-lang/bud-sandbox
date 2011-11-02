require 'rubygems'
require 'bud'
require 'test/unit'
require 'delivery/multicast'

module TestState
  include StaticMembership

  state do
    table :mcast_done_perm, [:ident] => [:payload]
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
    mc.add_member <+ [[mc2.ip_port], [mc3.ip_port]]
    mc.run_bg

    mc.sync_do{ mc.send_mcast <+ [[1, 'foobar']] }

    mc.sync_do{ assert_equal(1, mc.mcast_done_perm.length) }
    mc.sync_do{ assert_equal("foobar", mc.mcast_done_perm.first.payload) }
    mc.sync_do{ assert_equal(1, mc2.rcv_perm.length) }
    mc.sync_do{ assert_equal(1, mc3.rcv_perm.length) }
    mc.sync_do{ assert_equal("foobar", mc2.rcv_perm.first.payload) }

    mc.stop_bg
    mc2.stop_bg
    mc3.stop_bg
  end

  def test_reliable
    mc = RMC.new
    mc2 = RMC.new
    mc3 = RMC.new

    mc2.run_bg; mc3.run_bg
    mc.add_member <+ [[mc2.ip_port], [mc3.ip_port]]
    mc.run_bg

    resps = mc.sync_callback(mc.send_mcast.tabname, [[1, 'foobar']], mc.mcast_done.tabname)
    resps.each do |resp|
      assert_equal(resp.ident, 1)
    end
    assert_equal(mc2.rcv_perm.length, 1)
    assert_equal(mc3.rcv_perm.length, 1)
    assert(mc2.rcv_perm.include? [1, 'foobar'])
    assert(mc3.rcv_perm.include? [1, 'foobar'])

    mc.stop_bg
    mc2.stop_bg
    mc3.stop_bg
  end

end
