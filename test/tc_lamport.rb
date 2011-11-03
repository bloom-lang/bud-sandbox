require 'rubygems'
require 'bud'
require 'test/unit'
require 'ordering/lamport'

class LT
  include Bud
  include LamportClockManager
end

#this test suite should probably be more relaxed to instead ensure
#things like monotonic increasing lamport clocks instead of
#absolute values, but the absolute values suggested (enforced)
#here likely make debugging easier

class TestLamport < Test::Unit::TestCase
  def test_simple_encode_decode
    lt = LT.new
    lt.to_stamp <+ [['foo']]
    lt.run_bg
    lt.sync_do{
      assert_equal(["foo", LamportMsg.new(0, "foo")], lt.get_stamped.first)
      assert_equal(1, lt.get_stamped.length)
    }
    lt.sync_do{ lt.to_stamp <+ [['bar']] }
    lt.sync_do{
      assert_equal(["bar", LamportMsg.new(1, "bar")], lt.get_stamped.first)
      lt.retrieve_msg <+ [[lt.get_stamped.first[1]]]
    }
    lt.sync_do{ assert_equal([LamportMsg.new(1, "bar"), "bar"], lt.msg_return.first) }
    lt.stop
  end

  def test_advance_clocks
    lt = LT.new
    lt.to_stamp <+ [['foo']]
    lt.run_bg
    lt.sync_do{ assert_equal(["foo", LamportMsg.new(0, "foo")], lt.get_stamped.first) }
    lt.sync_do{ lt.retrieve_msg <+ [[LamportMsg.new(20, "long")]] }
    lt.sync_do{ lt.to_stamp <+ [["bar"]] }
    lt.sync_do{ assert_equal(["bar", LamportMsg.new(22, "bar")], lt.get_stamped.first) }
    lt.stop
  end
end
