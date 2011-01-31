require 'rubygems'
require 'bud'
require 'test/unit'
require 'heartbeat/heartbeat'

class HB < Bud
  include HeartbeatAgent

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

  def test_heartbeat_group
    hb = HB.new(:port => 46362, :visualize => 1, :dump => true)
    hb2 = HB.new(:port => 46363)
    hb3 = HB.new(:port => 46364)
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
