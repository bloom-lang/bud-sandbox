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

  def test_le
    v = LE.new("127.0.0.1", 10001, 1)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    v.member << ['127.0.0.1:10001', 1]
    v.current_state << ['election', "127.0.0.1:10001", 0] 
  
    #(0..2).each do |i|
    #  soft_tick(v)
    #end 

    sleep 6


    v.vote_status.each {|s| puts "VOTe StATUS: #{s.inspect}" } 

    assert_equal(1, v.current_state.length)
    v.current_state.each do |c|
      puts "CS: #{c.inspect}"
      ###assert_equal("leader", c.status)
    end
  end
  
end

