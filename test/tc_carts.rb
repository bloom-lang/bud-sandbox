require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/cart_workloads'
require 'cart/disorderly_cart'
require 'cart/destructive_cart'


module Remember
  state do
    table :memo, [:client, :server, :session, :array]
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

class LocalDisorderly
  include Bud
  include DisorderlyCart
  include CartClient
  include Remember
  include StaticMembership
end

class ReplDestructive
  include Bud
  include CartProtocol
  include DestructiveCart
  include ReplicatedKVS
  include BestEffortMulticast
  include StaticMembership
end

class LocalDestructive
  include Bud
  include CartClient
  include DestructiveCart
  include StaticMembership
  include BasicKVS
  include Remember
end

class TestCart < Test::Unit::TestCase
  include CartWorkloads

  def test_replicated_destructive_cart
    trc = false
    cli = TestCartClient.new(:port => 53524, :tag => "DESclient", :trace => trc)
    cli.run_bg
    prog = ReplDestructive.new(:port => 53525, :tag => "DESmaster", :trace => trc)
    rep = ReplDestructive.new(:port => 53526, :tag => "DESbackup", :trace => trc)
    rep2 = ReplDestructive.new(:port => 53527, :tag => "DESbackup2", :trace => trc)
    rep.run_bg
    # undo comment
    #rep2.run_bg
    cart_test_dist(prog, cli, rep) #, rep2)
    rep.stop
  end

  def test_replicated_disorderly_cart
    trc = false
    cli = TestCartClient.new(:tag => "DISclient", :trace => trc)
    cli.run_bg
    prog = BestEffortDisorderly.new(:port => 53525, :tag => "DISmaster", :trace => trc)
    rep = BestEffortDisorderly.new(:port => 53526, :tag => "DISbackup", :trace => trc)
    rep2 = BestEffortDisorderly.new(:port => 53527, :tag => "DISbackup2", :trace => trc)
    rep.run_bg
    #rep2.run_bg
    cart_test_dist(prog, cli, rep, rep2)
    rep.stop
  end

  def test_destructive_cart
    prog = LocalDestructive.new(:port => 32575, :tag => "dest")
    cart_test(prog)
  end

  def test_disorderly_cart
    prog = LocalDisorderly.new(:port => 23765, :tag => "dis")
    cart_test(prog)
  end

  def cart_test_dist(prog, cli, *others)
    cart_test_internal(prog, false, cli, *others)
  end

  def cart_test(prog)
    cart_test_internal(prog, true)
  end

  def cart_test_internal(program, dotest, client=nil, *others)
    ads = ([program] + others).map{|o| "#{program.ip}:#{o.port}"}
    add_members(program, ads)

    program.run_bg
    run_cart(program, client)

    cli = client.nil? ? program : client
    program.sync_do {
      assert_equal(1, cli.memo.length)
      # temporarily disabled.
      #assert_equal(4, cli.memo.first.array.length, "crap, i got #{cli.memo.first.inspect}") if dotest
    }
    program.stop
  end

  def add_members(b, hosts)
    hosts.each_with_index do |h, i|
      b.add_member <+ [[i, h]]
    end
  end
end
