require './test_common'
require 'test/cart_workloads'
require 'cart/cart_lattice'
require 'cart/disorderly_cart'
require 'cart/destructive_cart'
require 'cart/monotone_cart'

module Remember
  state do
    table :memo, response_msg.schema
  end

  bloom do
    memo <= response_msg
  end
end

class TestCartClient
  include Bud
  include CartClient
  include Remember
end

class BestEffortDisorderly
  include Bud
  include BestEffortMulticast
  include ReplicatedDisorderlyCart
  include StaticMembership
end

class LocalDisorderly < TestCartClient
  include DisorderlyCart
  include StaticMembership
end

class ReplDestructive
  include Bud
  include DestructiveCart
  include ReplicatedKVS
  include BestEffortMulticast
  include StaticMembership
end

class LocalDestructive < TestCartClient
  include DestructiveCart
  include StaticMembership
  include BasicKVS
end

class TestCart < Test::Unit::TestCase
  include CartWorkloads

  # XXX: currently broken (issue #1 in sandbox)
  def ntest_replicated_destructive_cart
    trc = false
    cli = TestCartClient.new(:tag => "DESclient", :trace => trc)
    cli.run_bg
    prog = ReplDestructive.new(:tag => "DESmaster", :trace => trc)
    rep = ReplDestructive.new(:tag => "DESbackup", :trace => trc)
    rep2 = ReplDestructive.new(:tag => "DESbackup2", :trace => trc)
    cart_test(prog, cli, rep, rep2)
  end

  def test_replicated_disorderly_cart
    trc = false
    cli = TestCartClient.new(:tag => "DISclient", :trace => trc)
    cli.run_bg
    prog = BestEffortDisorderly.new(:tag => "DISmaster", :trace => trc)
    rep = BestEffortDisorderly.new(:tag => "DISbackup", :trace => trc)
    rep2 = BestEffortDisorderly.new(:tag => "DISbackup2", :trace => trc)
    cart_test(prog, cli, rep, rep2)
  end

  def test_destructive_cart
    prog = LocalDestructive.new(:tag => "dest")
    cart_test(prog, prog)
  end

  def test_disorderly_cart
    prog = LocalDisorderly.new(:tag => "dis")
    cart_test(prog, prog)
  end

  def cart_test(primary, client, *others)
    nodes = [primary] + others
    nodes.each {|n| n.run_bg}
    addr_list = nodes.map {|n| "#{n.ip}:#{n.port}"}
    add_members(primary, addr_list)

    simple_workload(primary, client)
    multi_session_workload(primary, client)

    nodes.each {|n| n.stop}
    client.stop
  end

  def add_members(b, hosts)
    hosts.each_with_index do |h, i|
      b.sync_do {
        b.add_member <+ [[i, h]]
      }
    end
  end
end

class LocalCartLattice
  include Bud

  state do
    lcart :c
    lbool :done
    scratch :add_t, [:req] => [:item, :cnt]
    scratch :del_t, [:req] => [:item, :cnt]
    scratch :do_checkout, [:req] => [:lbound]
  end

  bloom do
    c <= add_t {|t| { t.req => [ACTION_OP, t.item,  t.cnt] } }
    c <= del_t {|t| { t.req => [ACTION_OP, t.item, -t.cnt] } }
    c <= do_checkout {|t| { t.req => [CHECKOUT_OP, t.lbound, ip_port] } }
    done <= c.is_complete
  end
end

class TestCartLattice < Test::Unit::TestCase
  def test_simple
    i = LocalCartLattice.new
    %w[c done del_t add_t do_checkout].each do |r|
      assert_equal(0, i.collection_stratum(r))
    end

    i.tick
    assert_equal(false, i.done.current_value.reveal)

    i.add_t <+ [[100, 5, 1], [101, 10, 4]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)

    i.do_checkout <+ [[103, 99]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)

    i.del_t <+ [[99, 3, 1]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)

    i.del_t <+ [[102, 10, 1]]
    i.tick
    assert_equal(true, i.done.current_value.reveal)
    assert_equal([[5 ,1], [10, 3]], i.c.current_value.summary)
    assert_equal(i.ip_port, i.c.current_value.checkout_addr)
  end

  # Current behavior is to raise an error if we see actions that follow the
  # checkout (in the ID sequence); such actions could instead be ignored.
  def test_action_follows_checkout
    i = LocalCartLattice.new
    i.add_t <+ [[200, 1, 1], [201, 1, 1], [202, 8, 20]]
    i.del_t <+ [[204, 8, 2]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)

    i.do_checkout <+ [[203, 200]]
    assert_raise(Bud::TypeError) do
      i.tick
    end
  end

  # Similarly, raise an error if the lower bound would require dropping some
  # messages; such messages could instead be ignored.
  def test_action_before_lbound
    i = LocalCartLattice.new
    i.add_t <+ [[200, 1, 1], [201, 1, 1], [202, 8, 20]]
    i.del_t <+ [[203, 8, 2]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)

    i.do_checkout <+ [[204, 201]]
    assert_raise(Bud::TypeError) do
      i.tick
    end
  end

  def test_extra_checkout
    i = LocalCartLattice.new
    i.add_t <+ [[300, 1, 1], [301, 2, 5]]
    i.do_checkout <+ [[303, 300]]
    i.tick

    assert_equal(false, i.done.current_value.reveal)
    i.do_checkout <+ [[302, 300]]
    assert_raise(Bud::TypeError) do
      i.tick
    end
  end

  def test_dup_checkout
    i = LocalCartLattice.new
    i.add_t <+ [[300, 1, 1], [301, 2, 5]]
    i.do_checkout <+ [[303, 300]]
    i.tick

    assert_equal(false, i.done.current_value.reveal)
    i.do_checkout <+ [[303, 300]]
    i.del_t <+ [[302, 2, 2]]
    i.tick

    assert_equal(true, i.done.current_value.reveal)
    assert_equal([[1, 1], [2, 3]], i.c.current_value.summary)
    assert_equal(i.ip_port, i.c.current_value.checkout_addr)
  end
end

class MReplicaProgram
  include Bud
  include MonotoneReplica
end

class MClientProgram
  include Bud
  include MonotoneClient

  state do
    table :response_log, response_msg.schema
  end

  bloom do
    response_log <= response_msg
  end
end

class TestMonotoneCart < Test::Unit::TestCase
  def test_monotone_simple
    s = MReplicaProgram.new
    c = MClientProgram.new
    [c, s].each {|n| n.run_bg}

    c.sync_do {
      c.serv <+ [[s.ip_port]]
      c.do_action <+ [[10, 1, 'beer', 1]]
      c.do_action <+ [[10, 2, 'vodka', 4]]
      c.do_checkout <+ [[10, 4, 1]]
    }

    sleep 0.1
    c.sync_do {
      assert(c.response_log.to_a.empty?)
    }

    c.sync_callback(:do_action, [[10, 3, 'beer', -1]], :response_msg)
    c.sync_do {
      assert_equal([[c.ip_port, s.ip_port, 10,
                     [['vodka', 4]]]], c.response_log.to_a)
    }

    [c, s].each {|n| n.stop}
  end

  def test_monotone_multi_session
    s = MReplicaProgram.new
    c = MClientProgram.new
    [c, s].each {|n| n.run_bg}

    c.sync_do {
      c.serv <+ [[s.ip_port]]
      c.do_action <+ [[10, 1, 'beer', 1], [11, 8, 'coffee', 2]]
      c.do_action <+ [[12, 2, 'gin', 3]]
    }

    c.sync_callback(:do_checkout, [[11, 9, 8]], :response_msg)
    c.sync_do {
      assert_equal([[c.ip_port, s.ip_port, 11,
                     [['coffee', 2]]]], c.response_log.to_a)
    }

    c.sync_do {
      c.do_checkout <+ [[12, 3, 1]]
    }
    c.sync_callback(:do_action, [[12, 1, 'rye', 1]], :response_msg)
    c.sync_do {
      assert_equal([[c.ip_port, s.ip_port, 11, [['coffee', 2]]],
                    [c.ip_port, s.ip_port, 12, [['gin', 3], ['rye', 1]]]],
                   c.response_log.to_a.sort)
    }

    [c, s].each {|n| n.stop}
  end
end
