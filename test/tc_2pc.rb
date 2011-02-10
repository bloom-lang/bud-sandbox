require '2pc/2pc'
require 'time_hack/time_moves'
require 'test/unit'


class TPCM < Bud
  include TwoPCMaster
end

class TPCA < Bud
  include TwoPCAgent
end 

class Test2PC < Test::Unit::TestCase
  def test_singlenode
    t = TPCM.new(:port => 12345)
    t2 = TPCA.new(:port => 12346)
    t3 = TPCA.new(:port => 12347)
    t.add_member <+ [['localhost:12346']]
    t.add_member <+ [['localhost:12347']]
    t.run_bg
    t2.run_bg
    #t3.run_bg

    t.sync_do{ t.request_commit <+ [[ 1, "foobar" ]] }
    t.sync_do{ 
      assert_equal(1, t.xact.length) 
      assert_equal("prepare", t.xact.first[2]) 
    }
    sleep 1

    t2.sync_do{ assert_equal(1, t2.waiting_ballots.length) }

    t2.sync_do{ t2.cast_vote <+ [[ 1, "Y" ]] }
    sleep 1


    return


    t.sync_do{ assert_equal(1, t.votes_rcvd.length) }
    t.sync_do{ 
      assert_equal(1, t.xact.length)
      assert_equal("prepare", t.xact.first[2]) 
    }
    t3.sync_do{ t3.cast_vote <+ [[ 1, "Y" ]] }
    sleep 1 

 
    t.sync_do {  
      t.xact.each {|x| puts "XACT: #{x.inspect}" }  
      assert_rqual(1, t.xact.length)
      assert_equal("commit", t.xact.first[2])
    }
  end
end
