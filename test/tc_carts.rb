require './test_common'
require 'test/cart_workloads'
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

class TestCart < MiniTest::Unit::TestCase
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
