require 'rubygems'
require 'bud'
require 'test/unit'
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
    prog = ReplDestructive.new(:port => 53525, :tag => "DESmaster", :trace => trc)
    rep = ReplDestructive.new(:port => 53526, :tag => "DESbackup", :trace => trc)
    rep2 = ReplDestructive.new(:port => 53527, :tag => "DESbackup2", :trace => trc)
    cart_test(prog, cli, rep) #, rep2)
  end

  def test_replicated_disorderly_cart
    trc = false
    cli = TestCartClient.new(:tag => "DISclient", :trace => trc)
    cli.run_bg
    prog = BestEffortDisorderly.new(:port => 53525, :tag => "DISmaster", :trace => trc)
    rep = BestEffortDisorderly.new(:port => 53526, :tag => "DISbackup", :trace => trc)
    rep2 = BestEffortDisorderly.new(:port => 53527, :tag => "DISbackup2", :trace => trc)
    cart_test(prog, cli, rep, rep2)
  end

  def test_destructive_cart
    prog = LocalDestructive.new(:port => 32575, :tag => "dest")
    cart_test(prog, prog)
  end

  def test_disorderly_cart
    prog = LocalDisorderly.new(:port => 23765, :tag => "dis")
    cart_test(prog, prog)
  end

  def cart_test(program, client, *others)
    nodes = [program] + others
    addr_list = nodes.map {|n| "#{program.ip}:#{n.port}"}
    add_members(program, addr_list)
    nodes.each {|n| n.run_bg}

    simple_workload(program, client)
    multi_session_workload(program, client)

    nodes.each {|n| n.stop}
    client.stop
  end

  def add_members(b, hosts)
    hosts.each_with_index do |h, i|
      b.add_member <+ [[i, h]]
    end
  end
end

class SimpleCheckout
  include Bud

  state do
    lcart :c
    lbool :done
    lset :contents
    scratch :add_t, [:req] => [:item, :cnt]
    scratch :del_t, [:req] => [:item, :cnt]
    scratch :do_checkout, [:req] => [:lbound]
  end

  bloom do
    c <= add_t {|t| { t.req => [ACTION_OP, [t.item, t.cnt]] } }
    c <= del_t {|t| { t.req => [ACTION_OP, [t.item, -t.cnt]] } }
    c <= do_checkout {|t| { t.req => [CHECKOUT_OP, t.lbound] } }
    done <= c.cart_done
    contents <= c.contents
  end
end

class TestCheckoutLattice < Test::Unit::TestCase
  def test_simple
    i = SimpleCheckout.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:c, :done, :del_t, :add_t, :do_checkout].each do |r|
      assert(strat_zero.include? r)
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

    puts i.contents.current_value.reveal.inspect
  end
end
