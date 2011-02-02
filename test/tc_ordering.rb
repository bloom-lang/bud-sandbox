require 'rubygems'
require 'test/unit'
require 'bud'
require 'ordering/serializer'
require 'ordering/nonce'
require 'time_hack/time_moves'
require 'ordering/assigner'

class ST < Bud
  include Serializer
  
  def state
    super
    periodic :tic, 1    
    table :mems, ['reqid', 'ident', 'payload']
  end 

  declare 
  def remem
    mems <= dequeue_resp
  end
end

class AS < Bud
  include AggAssign
end

class SN < Bud
  include SimpleNonce
  include TimeMoves
end

class GN < Bud
  include GroupNonce
  include StaticMembership
  include TimeMoves
end

class TestSer < Test::Unit::TestCase

  def test_group_nonce
    gn = GN.new(:visualize => 3, :dump => true)
    gn.my_id <+ [[1]]
    gn.add_member <+ [['foo', 1]]
    gn.add_member <+ [['bar', 2]]
    gn.add_member <+ [['baz', 3]]
    gn.seed <+ [[nil]]
    
    gn.run_bg

    (0..7).each do |t|
      sleep 1
      puts "NON: #{gn.nonce.first.inspect}"
      #gn.member.each{ |m| puts "EM: #{m.inspect}" }
    end
  end

  def nntest_assn
    as = AS.new
    as.run_bg

    sleep 10
  end
  
  def ntest_simple_nonce
    sn = SN.new(:dump => true, :port => 235235)
    sn.run_bg

    old_val = 0
    (0..10).each do |i|
      sn.nonce.each {|n| puts "NON: #{n}" } 
      val = sn.nonce.first.ident
      assert(val != old_val, "nonce had same value: #{old_val} (on run #{i})")
      old_val = val
      sleep 2
      # doesn't work.
      #sn.localtick <~ [['1234']]
    end
  end 

  def ntest_assigner
    as = AS.new
    as.run_bg
  end

  def ntest_serialization
    st = ST.new
    st.run_bg

    st.enqueue <+ [[1, 'foo']]
    st.enqueue <+ [[2, 'bar']]
    st.enqueue <+ [[3, 'baz']]
    st.dequeue <+ [[1234]]
    sleep 3

    assert_equal(1, st.mems.length)
    assert_equal([1234, 1, 'foo'], st.mems.first)
    st.dequeue <+ [[2345]]
    sleep 3
    assert_equal(2, st.mems.length)
    st.mems.each_with_index do |m, i|
      case i
        when 0 then assert_equal([1234, 1, 'foo'], m)
        when 1 then assert_equal([2345, 2, 'bar'], m)
      end
    end
  end
end
