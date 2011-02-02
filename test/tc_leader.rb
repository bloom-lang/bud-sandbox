require 'rubygems'
require 'bud'
require 'test/unit'
require 'time_hack/time_moves'
#require 'paxos/leader'
require 'paxos/le3'

class LE < Bud
  include LeaderElection
  include TimeMoves
end 

class TestLE < Test::Unit::TestCase

  def ntest_le
    v = LE.new(:port => 10001, :visualize => 3)
    v.add_member <+ [['127.0.0.1:10001', 1]]
    v.my_id <+ [[1]]
    v.seed <+ [[nil]]
    v.init_le <+ [[nil]]
    v.current_state << ['election', "127.0.0.1:10001", 0, Time.new.to_f, 0.5] 
  
    assert_nothing_raised(RuntimeError) {v.run_bg}
    sleep 6

    v.vote_status.each {|s| puts "VOTe StATUS: #{s.inspect}" } 

    assert_equal(1, v.current_state.length)
    v.current_state.each do |c|
      puts "CS: #{c.inspect}"
      assert_equal("leader", c.state)
    end
  end

  def startup(ip, port, id)
    rt = LE.new(:ip => ip, :port => port, :visualize => 3)
    rt.add_member <+ [['localhost:20001']]
    rt.add_member <+ [['localhost:20002']]
    #rt.add_member <+ [['localhost:20003']]
    rt.my_id <+ [[id]]
    rt.seed <+ [[nil]]
    rt.init_le <+ [[nil]]
    rt.current_state << ['election', "#{ip}:#{port}", id, Time.new.to_f, 0.5 ] 
    rt.run_bg
    sleep 1
    return rt
  end

  def test_le_dist

    v = startup("localhost", 20001, 1)  
    v2 = startup("localhost", 20002, 2)  
    #v3 = startup("localhost", 20003, 3)  

    sleep 16

    puts "GOT 1: #{v.current_state.first.inspect}"
    puts "GOT 2: #{v2.current_state.first.inspect}"
    #puts "GOT : #{v3.current_state.first.inspect}"
  end
  
end

