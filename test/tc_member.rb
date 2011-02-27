require 'rubygems'
require 'test/unit'
require 'bud'
require 'membership/membership'
require 'time_hack/time_moves'

class MT
  include Bud
  include StaticMembership
  include TimeMoves
  
  bootstrap do
    add_member <+ [['arr', 1]]
    add_member <+ [['farr', 2]]
  end
end

class MT2
  include Bud
  include StaticMembership
  include TimeMoves
  
  bootstrap do
    add_member <+ [['arr', 1]]
    add_member <+ [['farr', 2]]
  end
end


class TestMembership < Test::Unit::TestCase

  def test_mem1
    mt = MT.new
    mt.run_bg
    mt.add_member <+ [['foo', 7]]
    sleep 3
    assert_equal(2, mt.member.length)
    assert(!( mt.member.map{|m| m.host}.include? "foo"))
    mt.stop_bg
  end
  
  def test_mem2
    mt = MT2.new
    # should be same as bootstrapping.
    mt.add_member <+ [['arr', 1]]
    mt.add_member <+ [['farr', 2]]

    mt.run_bg
    mt.add_member <+ [['foo', 7]]
    sleep 3
    assert_equal(2, mt.member.length)
    assert(!( mt.member.map{|m| m.host}.include? "foo"))

    mt.stop_bg
  end
end
