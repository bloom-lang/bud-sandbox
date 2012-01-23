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

class CCli
  include Bud
  include CartClient
  include Remember
end


class BCS
  include Bud
  include BestEffortMulticast
  include ReplicatedDisorderlyCart
  include StaticMembership
end

class LclDis
  include Bud
  include DisorderlyCart
  include CartClient
  include Remember
  include StaticMembership
end


class DCR
  include Bud
  #include TracingExtras
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
  include Remember

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
    trc = false
    cli = CCli.new(:port => 53524, :tag => "DESclient", :trace => trc)
    cli.run_bg
    prog = DCR.new(:port => 53525, :tag => "DESmaster", :trace => trc, :dump_rewrite => true)
    rep = DCR.new(:port => 53526, :tag => "DESbackup", :trace => trc)
    rep2 = DCR.new(:port => 53527, :tag => "DESbackup2", :trace => trc)
    rep.run_bg
    # undo comment
    #rep2.run_bg
    cart_test_dist(prog, cli, rep) #, rep2)
    rep.stop
  end

  def test_replicated_disorderly_cart
    trc = false
    cli = CCli.new(:tag => "DISclient", :trace => trc)
    cli.run_bg
    prog = BCS.new(:port => 53525, :tag => "DISmaster", :trace => trc)
    rep = BCS.new(:port => 53526, :tag => "DISbackup", :trace => trc)
    rep2 = BCS.new(:port => 53527, :tag => "DISbackup2", :trace => trc)
    rep.run_bg
    #rep2.run_bg
    cart_test_dist(prog, cli, rep, rep2)
    rep.stop
  end

  def test_destructive_cart
    prog = DummyDC.new(:port => 32575, :tag => "dest")
    cart_test(prog)
  end

  def test_disorderly_cart
    program = LclDis.new(:port => 23765, :tag => "dis")
    cart_test(program)
  end

  def cart_test_dist(prog, cli, *others)
    cart_test_internal(prog, false, cli, *others)
  end

  def cart_test(prog)
    cart_test_internal(prog, true)
  end

  def cart_test_internal(program, dotest, client=nil, *others)
    ads = ([program] + others).map{|o| "#{program.ip}:#{o.port}"}
    puts "ADS is #{ads.inspect} #{ads.class}"
    add_members(program, *ads)
    
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

  def add_members(b, *hosts)
    hosts.each_with_index do |h, i|
      puts "ADD: #{i}, #{h}"
      b.add_member <+ [[i, h]]
    end
  end
end
