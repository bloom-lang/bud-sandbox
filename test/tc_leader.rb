require 'rubygems'
require 'bud'
require 'test/unit'
require 'time_hack/time_moves'
require 'paxos/leader'

class LE < Bud
  include LeaderElection
  include TimeMoves
end 

class TestLE < Test::Unit::TestCase

  def ntest_le
    v = LE.new("127.0.0.1", 10001, 1)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    v.member << ['127.0.0.1:10001', 1]
    v.current_state << ['election', "127.0.0.1:10001", 0] 
  
    sleep 6

    v.vote_status.each {|s| puts "VOTe StATUS: #{s.inspect}" } 

    assert_equal(1, v.current_state.length)
    v.current_state.each do |c|
      puts "CS: #{c.inspect}"
      assert_equal("leader", c.status)
    end
  end

  def startup(ip, port, id)
    rt = LE.new(ip, port, id)
    rt.run_bg
    rt.member << ['localhost:20001']
    rt.member << ['localhost:20002']
    rt.member << ['localhost:20003']

    rt.current_state << ['election', "#{ip}:#{port}", 0] 
    return rt
  end

  def test_le_dist

    v = startup("localhost", 20001, 1)  
    v2 = startup("localhost", 20002, 2)  
    v3 = startup("localhost", 20003, 3)  

    sleep 6

    puts "GOT : #{v.current_state.first.inspect}"
    puts "GOT : #{v2.current_state.first.inspect}"
    puts "GOT : #{v3.current_state.first.inspect}"
  end
  
end

