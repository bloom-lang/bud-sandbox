require 'rubygems'
require 'test/unit'
require 'bud'
require 'ordering/serializer'

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

class TestSer < Test::Unit::TestCase

  def test_serialization
    st = ST.new('localhost', 648212, {})
    st.run_bg

    st.enqueue <= [[1, 'foo']]
    st.enqueue <= [[2, 'bar']]
    st.enqueue <= [[3, 'baz']]

    st.dequeue <= [[1234]]
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
