require 'rubygems'
require 'bud'
require 'test/unit'
require 'delivery/multicast'

module TestState
  include StaticMembership

  state {
    table :mcast_done_perm, [:ident] => [:payload]
    table :rcv_perm, [:ident] => [:payload]
  }

  declare
  def mem
    mcast_done_perm <= mcast_done.map{|d| d }
    rcv_perm <= pipe_chan.map{|r| [r.ident, r.payload] }
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
    mc = MC.new(:port => 34256)
    mc2 = MC.new(:port =>  34257)
    mc3 = MC.new(:port =>  34258)

    mc.add_member <+ [["localhost: 34257" ]]
    mc.add_member <+ [["localhost: 34258" ]]

    assert_nothing_raised(RuntimeError) { mc.run_bg; mc2.run_bg; mc3.run_bg }

    mc.sync_do{ mc.send_mcast <+ [[1, 'foobar']] }

    mc.sync_do{ assert_equal(1, mc.mcast_done_perm.length) }
    mc.sync_do{ assert_equal("foobar", mc.mcast_done_perm.first.payload) }
    mc.sync_do{ assert_equal(1, mc2.rcv_perm.length) }
    return
    mc.sync_do{ assert_equal(1, mc3.rcv_perm.length) }
    mc.sync_do{ assert_equal("foobar", mc2.rcv_perm.first.payload) }

    mc.stop_bg
    mc2.stop_bg
    mc3.stop_bg
  end
end
