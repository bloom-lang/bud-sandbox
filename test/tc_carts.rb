require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/cart_workloads'
require 'cart/disorderly_cart'
require 'cart/destructive_cart'


class CCli
  include Bud
  include CartClient
 
  state do
    table :memo, [:client, :server, :session, :array]
  end

  bloom :memm do
    memo <= response_msg
    #stdio <~ client_action.inspected
  end
end


class BCS
  include Bud
  include BestEffortMulticast
  include ReplicatedDisorderlyCart
  include CartClient
  #include Remember
end


class DCR
  include Bud
  #include CartClientProtocol
  #include CartClient
  include CartProtocol
  include DestructiveCart
  include ReplicatedKVS
  include BestEffortMulticast
  include StaticMembership
  #include Remember
end

class DummyDC
  include Bud
  include CartClientProtocol
  include CartClient
  include CartProtocol
  include DestructiveCart
  include StaticMembership
  include BasicKVS
  #include Remember

  state do
    table :members, [:peer]
  end
end

class BCSC
  include Bud
  include CartClient

  state do
    table :cli_resp_mem, [:@client, :server, :session, :item, :cnt]
  end

  bloom :memmy do
    cli_resp_mem <= response_msg
  end
end

class TestCart < Test::Unit::TestCase
  include CartWorkloads

  def test_replicated_destructive_cart
    cli = CCli.new(:tag => "client", :trace => true)
    cli.run_bg
    prog = DCR.new(:port => 53525, :tag => "master", :trace => true)
    rep = DCR.new(:port => 53526, :tag => "backup", :trace => true)
    rep.run_bg
    cart_test(prog, cli, rep)
  end

  def ntest_destructive_cart
    prog = DummyDC.new(:port => 32575, :tag => "dest", :trace => true)
    cart_test(prog)
  end

  def ntest_disorderly_cart
    program = BCS.new(:port => 23765, :tag => "dis", :trace => true)
    cart_test(program)
  end

  def cart_test(program, client=nil, *others)
    addy = "#{program.ip}:#{program.port}"
    add_members(program, addy)
    others.each do |o|
      addy = "#{program.ip}:#{o.port}"
      puts "add #{addy} to members"
      add_members(program, addy)
    end
    program.run_bg
    run_cart(program, client)
    
    program.sync_do {
      assert_equal(1, client.memo.length)
      puts "I got #{client.memo.first.array.inspect}"
      assert_equal(4, client.memo.first.array.length)
    }
    program.stop_bg
  end

  def add_members(b, *hosts)
    hosts.each do |h|
      b.add_member <+ [[h]]
    end
  end
end
