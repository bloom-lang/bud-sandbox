require '2pc/2pc'
require 'test/unit'


class TPCM
  include Bud
  include TwoPCMaster
end

class TPCA
  include Bud
  include TwoPCAgent
end

class Test2PC < Test::Unit::TestCase
  def test_singlenode
    t = TPCM.new(:port => 32345, :tag => "master", :trace => false)
    t2 = TPCA.new(:port => 32346, :tag => "a1", :trace => false)
    t3 = TPCA.new(:port => 32347, :tag => "a2", :trace => false)
    t.add_member <+ [['localhost:32346']]
    t.add_member <+ [['localhost:32347']]
    t.run_bg
    t2.run_bg
    t3.run_bg

    t.sync_do{ t.request_commit <+ [[ 1, "foobar" ]] }
    t.sync_do{
      assert_equal(1, t.xact.length)
      assert_equal("prepare", t.xact.first[2])
    }

    t2.sync_do{ assert_equal(1, t2.waiting_ballots.length) }

    t2.sync_do{ t2.cast_vote <+ [[ 1, "Y" ]] }

    t.delta(:vote)
    t.sync_do{ assert_equal(1, t.votes_rcvd.length) }
    t.sync_do{
      assert_equal(1, t.xact.length)
      assert_equal("prepare", t.xact.first[2])
    }
    t3.sync_do{ t3.cast_vote <+ [[ 1, "Y" ]] }

    t.delta(:decide)
    t.sync_do

    t.sync_do do
      assert_equal(1, t.vote_status.length)
      assert_equal("Y", t.vote_status.first[2])
    end

    t.sync_do {
      assert_equal(1, t.xact.length)
      assert_equal("commit", t.xact.first[2])
    }
  end
end
