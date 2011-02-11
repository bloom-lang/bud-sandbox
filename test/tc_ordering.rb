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
    table :mems, [:reqid, :ident, :payload]
  end 

  declare 
  def remem
    mems <= dequeue_resp
  end
end

class SN < Bud
  include SimpleNonce
end

class GN < Bud
  include GroupNonce
  include StaticMembership
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

    (0..30).each do |t|
      rem = nil
      gn.sync_do{ rem = gn.nonce.first }
      assert_equal(t * 3 + 1, rem[0])
    end
    gn.stop_bg
  end

  def test_simple_nonce
    sn = SN.new(:dump => true, :port => 235235)
    sn.run_bg

    old_val = 0
    (0..10).each do |i|
      sn.sync_do { 
        val = sn.nonce.first.ident
        assert(val != old_val, "nonce had same value: #{old_val} (on run #{i})")
        old_val = val
      }
      # doesn't work.
      #sn.localtick <~ [['1234']]
    end
    sn.stop_bg
  end 

  def test_serialization
    st = ST.new(:visualize => 3)
    st.run_bg

    st.async_do{ st.enqueue <+ [[1, 'foo']] } 
    st.async_do{ st.enqueue <+ [[2, 'bar']] }
    s.async_do{ st.enqueue <+ [[3, 'baz']] }
    st.sync_do{ }
    st.sync_do{ 
      st.dequeue <+ [[1234]] 
    }
    st.sync_do{ }
    st.sync_do {
      assert_equal(1, st.mems.length) 
      assert_equal([1234, 1, 'foo'], st.mems.first)
    }

    st.sync_do{ st.dequeue <+ [[2345]] }
    st.sync_do{ }
    st.sync_do {
      assert_equal(2, st.mems.length)
      st.mems.each_with_index do |m, i|
        case i
          when 0 then assert_equal([1234, 1, 'foo'], m)
          when 1 then assert_equal([2345, 2, 'bar'], m)
        end
      end
    }
    st.stop_bg
  end
end
