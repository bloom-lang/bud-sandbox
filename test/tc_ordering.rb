require 'rubygems'
require 'test/unit'
require 'bud'
require 'ordering/serializer'
require 'ordering/nonce'
require 'time_hack/time_moves'
#require 'ordering/assigner'

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

#class AS < Bud
#  include Assigner
#end

class SN < Bud
  include SimpleNonce
  include TimeMoves
end

class TestSer < Test::Unit::TestCase
  
  def test_simple_nonce
    sn = SN.new("localhost", 235636, {'dump' => true})
    sn.run_bg

    old_val = 0
    (0..20).each do |i|
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
    as = AS.new('localhost', 82357, {})
    as.run_bg
  end

  def test_serialization
    st = ST.new('localhost', 648212, {'dump' => true})
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
