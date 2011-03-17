require 'rubygems'
require 'bud'
require 'test/unit'
require 'heartbeat/heartbeat'

class HB
  include Bud
  include HeartbeatAgent
  include StaticMembership

  bootstrap do
    payload <= [['foo']]
    payload <= [['foo']]
    payload <= [['foo']]
    add_member <= [[ "localhost:46362" ]]
    add_member <= [[ "localhost:46363" ]]
    add_member <= [[ "localhost:46364" ]]
  end
end


class TestHB < Test::Unit::TestCase

  def test_heartbeat_group
    hb = HB.new(:port => 46362, :dump => true, :trace => true)
    hb2 = HB.new(:port => 46363)
    hb3 = HB.new(:port => 46364)
    hb.payload << ['foo']
    hb2.payload << ['foo']
    hb3.payload << ['foo']
    hb.run_bg
    hb2.run_bg
    hb3.run_bg


    sleep 16

    [hb, hb2, hb3].each do |h|
      h.sync_do { 
        #assert_equal(3, h.last_heartbeat.length) 
      }
      s = nil
      h.sync_do{ s = h.last_heartbeat.map{|b| b.peer } }
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
    hb.stop_bg
    hb2.stop_bg
    hb3.stop_bg
  end
end
