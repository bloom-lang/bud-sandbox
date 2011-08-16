require 'rubygems'
require 'bud'
require 'test/unit'
require 'ordering/lamport'

class LT
  include Bud
  include LamportClockManager
end

class TestLamport < Test::Unit::TestCase
  def test_simple_encode_decode
    lt = LT.new
    lt.to_stamp <+ [['foo']]
    lt.run_bg
    lt.sync_do{
      assert_equal(["foo", [[0, "foo"]]], lt.get_stamped.first)
      assert_equal(1, lt.get_stamped.length)
    }
    lt.sync_do{ lt.to_stamp <+ [['bar']] }
    lt.sync_do{
      assert_equal(["bar", [[1, "bar"]]], lt.get_stamped.first)
      lt.retrieve_msg <+ [lt.get_stamped.first[1]]
    }
    lt.sync_do{ assert_equal([[1, "bar"], "bar"], lt.msg_return.first) }
    lt.stop_bg
  end

  def test_advance_clocks
    lt = LT.new
    lt.to_stamp <+ [['foo']]
    lt.run_bg
    lt.sync_do{ assert_equal(["foo", [[0, "foo"]]], lt.get_stamped.first) }
    lt.sync_do{ lt.retrieve_msg <+ [[[20, "long"]]] }
    lt.sync_do{ lt.to_stamp <+ [['bar']] }
    lt.sync_do{ assert_equal(["bar", [[21, "bar"]]], lt.get_stamped.first) }
    lt.stop_bg
  end
end
