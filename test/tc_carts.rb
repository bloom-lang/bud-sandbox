require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/cart_workloads'
require 'time_hack/time_moves'
require 'cart/disorderly_cart'
require 'cart/destructive_cart'



module Remember
  include Anise
  annotator :declare
  def state
    super
    table :memo, [:client, :server, :session, :array]
  end

  declare 
  def memm
    memo <= response_msg.map{|r| r }
  end
end


class BCS < Bud
  include BestEffortMulticast
  include ReplicatedDisorderlyCart
  include CartClient
  include Remember
end

class DCR < Bud
  include CartClientProtocol
  include CartClient
  include CartProtocol
  include DestructiveCart
  include ReplicatedKVS
  include BestEffortMulticast
  include Remember
end

class DummyDC < Bud
  include CartClientProtocol
  include CartClient
  include CartProtocol
  include DestructiveCart
  include BasicKVS
  include Remember

  def state
    super
    table :members, [:peer]
  end
end

class BCSC < Bud
  include CartClient
  def state
    super
    table :cli_resp_mem, [:@client, :server, :session, :item, :cnt]
  end

  declare 
  def memmy
    cli_resp_mem <= response_msg.map{|r| r }
  end
end

class TestCart < Test::Unit::TestCase
  include CartWorkloads

  def test_disorderly_cart
    program = BCS.new(:port => 23765, :dump => true, :visualize => 3)
    #program = BCS.new(:port => 23765, :dump => true)
    #program = DummyDC.new('localhost', 23765, {'dump' => true})
    #program = DCR.new('localhost', 23765, {'dump' => true, 'scoping' => true})

    addy = "#{program.ip}:#{program.port}"
    add_members(program, addy)
    program.run_bg
    run_cart(program)

    sleep 4
    #program.memo.each {|m| puts "MEMO: #{m.inspect}" }

    program.sync_do{ 
      assert_equal(1, program.memo.length) 
      #program.memo.each {|m| puts "MEMO: #{m.inspect}" }
      assert_equal(2, program.memo.first.array.length) 
    }
  end

  def add_members(b, *hosts)
    hosts.each do |h|
      assert_nothing_raised(RuntimeError) { b.add_member <+ [[h]] }
    end
  end


end
