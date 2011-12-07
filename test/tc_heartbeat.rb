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
    port_list = [46362, 46363, 46364]
    hb_list = port_list.map {|p| HB.new(:port => p)}
    hb_list.each {|h| h.run_bg}

    # wait for the heartbeats to start appearing
    hb_list.first.delta(:last_heartbeat)

    hb_list.each do |h|
      h.sync_do {
#        assert_equal(3, h.last_heartbeat.length)
      }
      s = nil
      h.sync_do { s = h.last_heartbeat.map {|b| b.peer } }
      hosts = []
      port_list.each do |p|
        if h.port != p
          hosts << "#{h.ip}:#{p}"
        end
      end

      hosts.each do |c|
        assert(s.include?(c), "missing host #{c.inspect}")
      end
    end

    hb_list.each {|h| h.stop}
  end
end
