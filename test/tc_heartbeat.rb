require 'rubygems'
require 'bud'
require 'test/unit'
require 'heartbeat/heartbeat'

class HB < Bud
  include Heartbeat

  def state
    super
    #channel :tickler, ['@host']
    #periodic :tix, 1
  end

  def bootstrap
    peers << [ "localhost:46362" ]
    peers << [ "localhost:46363" ]
    peers << [ "localhost:46364" ]
  end
end


class TestHB < Test::Unit::TestCase

  def ntest_basic_heartbeat
    hb = HB.new("localhost", 46364, {'visualize' => true, 'dump' => true})
    hb.run_bg
    sleep 100
  end
  
  def test_heartbeat_group
    hb = HB.new("localhost", 46362, {'visualize' => true, 'dump' => true})
    hb2 = HB.new("localhost", 46363, {})
    hb3 = HB.new("localhost", 46364, {})


    hb.run_bg
    hb2.run_bg
    hb3.run_bg


    sleep 14

    [hb, hb2, hb3].each do |h|
      assert_equal(2, h.last_heartbeat.length)
      s = h.last_heartbeat.map{|b| b.peer }
      hosts = []
      [46362, 46363, 46364].each do |b|
        if h.port != b
          hosts << "localhost:#{b}"
        end
      end

      hosts.each do |c|
        assert(s.include? c)
      end
    end

  end
end
