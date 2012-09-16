require './test_common'
require 'test/cart_workloads'
require 'cart/cart_lattice'
require 'cart/disorderly_cart'
require 'cart/destructive_cart'


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

  # XXX: currently broken
  def ntest_replicated_destructive_cart
    trc = false
    cli = TestCartClient.new(:tag => "DESclient", :trace => trc)
    cli.run_bg
    prog = ReplDestructive.new(:tag => "DESmaster", :trace => trc)
    rep = ReplDestructive.new(:tag => "DESbackup", :trace => trc)
    rep2 = ReplDestructive.new(:tag => "DESbackup2", :trace => trc)
    cart_test(prog, cli, rep) #, rep2)
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

  def cart_test(program, client, *others)
    nodes = [program] + others
    nodes.each {|n| n.run_bg}
    addr_list = nodes.map {|n| "#{program.ip}:#{n.port}"}
    add_members(program, addr_list)

    simple_workload(program, client)
    multi_session_workload(program, client)

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
