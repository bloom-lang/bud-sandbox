require 'rubygems'
require 'test/unit'
require 'bud'
require 'membership/membership'
require 'time_hack/time_moves'

class MT < Bud
  include StaticMembership
  include TimeMoves
  
  def bootstrap
    add_member <+ [['arr', 1]]
    add_member <+ [['farr', 2]]
  end
end


class MT2 < Bud
  include StaticMembership
  include TimeMoves
  
  def bootstrap
    add_member <+ [['arr', 1]]
    add_member <+ [['farr', 2]]
  end
end


class TestMembership < Test::Unit::TestCase

  def test_mem1
    mt = MT.new(:visualize => 3, :dump => true)
    mt.run_bg
    mt.add_member <+ [['foo', 7]]
    sleep 3
    assert_equal(2, mt.member.length)
    assert(!( mt.member.map{|m| m.host}.include? "foo"))
  end
  
  def test_mem2
    mt = MT2.new(:visualize => 3, :dump => true)
    # should be same as bootstrapping.
    mt.add_member <+ [['arr', 1]]
    mt.add_member <+ [['farr', 2]]

    mt.run_bg
    mt.add_member <+ [['foo', 7]]
    sleep 3
    assert_equal(2, mt.member.length)
    assert(!( mt.member.map{|m| m.host}.include? "foo"))
  end
end
