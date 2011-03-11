require 'voting/voting'
require 'test/unit'

class VM
  include Bud
  include VotingMaster
end

class VA
  include Bud
  include VotingAgent
end

class VA2 < VA
  # override the default
  declare
  def decide
    cast_vote <= waiting_ballots.map{|b| b if 1 == 2 }
  end
end

class TestVoting < Test::Unit::TestCase
	
  def initialize(args)
    @opts = {}
    super
  end

  def start_kind(kind, port)
    t = nil
    assert_nothing_raised(RuntimeError) { eval "t = #{kind}.new(@opts.merge(:port => #{port}, :tag => #{port}))" }
    return t
  end

  def start_three(one, two, three, kind)
    t = VM.new(@opts.merge(:port => one))
    t2 = start_kind(kind, two)
    t3 = start_kind(kind, three)

    t.add_member <+ [["localhost:#{two.to_s}"]]
    t.add_member <+ [["localhost:#{three.to_s}"]]

    t.run_bg
    t2.run_bg
    t3.run_bg

    return [t, t2, t3]
  end

  def test_votingpair
    (t, t2, t3) = start_three(12346, 12347, 12348, "VA")

    t.sync_do{ t.begin_vote <+ [[1, 'me for king']] }
    t2.sync_do{}
    t2.sync_do{ assert_equal([1,'me for king', 'localhost:12346'], t2.waiting_ballots.first) }
    t3.sync_do{ assert_equal([1,'me for king', 'localhost:12346'], t3.waiting_ballots.first) }
    t.sync_do{ assert_equal([1, 'yes', 2], t.vote_cnt.first) }
    t.sync_do{ assert_equal([1, 'me for king', 'yes'], t.vote_status.first) }

    t.stop_bg
    t2.stop_bg
    t3.stop_bg
  end

  def test_votingpair2
    (t, t2, t3) = start_three(12316, 12317, 12318, "VA2")

    t.sync_do{ t.begin_vote <+ [[1, 'me for king']] }
    t2.sync_do{}
    t2.sync_do{ assert_equal([1,'me for king', 'localhost:12316'], t2.waiting_ballots.first) }
    t3.sync_do{ assert_equal([1,'me for king', 'localhost:12316'], t3.waiting_ballots.first) }
    t2.sync_do{ t2.cast_vote <+ [[1, "hell yes"]] }
    t.sync_do{}

    t.sync_do{ assert_equal([1, 'hell yes', 1], t.vote_cnt.first) }
    t.sync_do{ assert_equal([1, 'me for king', 'in flight'], t.vote_status.first) }
    t3.sync_do{ t3.cast_vote <+ [[1, "hell yes"]] }
    t.sync_do{}

    t.sync_do{ assert_equal([1, 'hell yes', 2], t.vote_cnt.first) }
    t.sync_do{ assert_equal([1, 'me for king', 'hell yes'], t.vote_status.first) }
    t.stop_bg
    t2.stop_bg
    t3.stop_bg
  end
end
