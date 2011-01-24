require '2pc/2pc'
require 'time_hack/time_moves'
require 'test/unit'


class TPCM < Bud
  include TwoPCMaster
  include TimeMoves
end

class TPCA < Bud
  include TwoPCAgent
  include TimeMoves
end 

class Test2PC < Test::Unit::TestCase
  def test_singlenode
    t = TPCM.new('localhost', 12345, {'visualize' => true})
    t2 = TPCA.new('localhost', 12346, {'visualize' => true})
    t3 = TPCA.new('localhost', 12347, nil)
    t.run_bg
    t2.run_bg
    t3.run_bg
    t.member << ['localhost:12346']
    t.member << ['localhost:12347']
    t.request_commit <+ [[ 1, "foobar" ]]

    sleep 2

    assert_equal(1, t.xact.length)
    assert_equal("prepare", t.xact.first[2])

    assert_equal(1, t2.waiting_ballots.length)

    t2.cast_vote <+ [[ 1, "Y" ]]
    sleep 1
    assert_equal(1, t.votes_rcvd.length)
    assert_equal("prepare", t.xact.first[2])
    t3.cast_vote <+ [[ 1, "Y" ]]
    sleep 2 
  
    t.xact.each {|x| puts "XACT: #{x.inspect}" }  
    
    assert_equal("commit", t.xact.first[2])
  end
end
