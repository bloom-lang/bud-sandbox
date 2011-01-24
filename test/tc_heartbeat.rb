require 'rubygems'
require 'bud'
require 'test/unit'
require 'heartbeat/heartbeat'

class HB < Bud
  include Heartbeat

  def state
    super
    #channel :tickler, ['@host']
  end

  def bootstrap
    peers << [ "localhost:46362" ]
    peers << [ "localhost:46363" ]
    peers << [ "localhost:46364" ]
  end
end


class TestHB < Test::Unit::TestCase

  def advance(p)
    advancer(p.ip, p.port)
  end

  def advancer(ip, port)
    sleep 1
    send_channel(ip, port, "tickler", ["#{ip}:#{port}"])
  end

  def send_channel(ip, port, chan, payload)
    EventMachine::connect(ip, port) do |c|
      pl = ([chan, payload]).to_msgpack
      assert_nothing_raised(RuntimeError) { c.send_data(pl) }
    end
  end

  def ntest_basic_heartbeat
    hb = HB.new("localhost", 46364, {'visualize' => true, 'dump' => true})
    hb.run_bg
    sleep 100
  end
  
  def test_heartbeat_group
    hb = HB.new("localhost", 46362, {'visualize' => true, 'dump' => true})
    hb2 = HB.new("localhost", 46363, {})
    #hb3 = HB.new("localhost", 46364, {})


    hb.run_bg
    hb2.run_bg
    #hb3.run_bg


    sleep 10

    (0..50).each do |i|
      hb.last_heartbeat.each do |h|
        puts i.to_s +  ":" + hb.budtime.to_s + " LAST: #{h.inspect}"
      end
      sleep 1
    end
    


    #sleep 20

    puts "OK" 

    #hb2.heartbeat_log.each {|l| puts "log: #{l.inspect}" } 
  end
end
