require './test_common'
require 'voting/voting'

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
  bloom :decide do
    cast_vote <= waiting_ballots {|b| b if 1 == 2 }
  end
end

class TestVoting < Test::Unit::TestCase
	
  def initialize(args)
    @opts = {}
    super
  end

  def start_kind(kind, port)
    t = nil
    eval "t = #{kind}.new(@opts.merge(:port => #{port}, :tag => #{port}, :trace => false))"
    return t
  end

  def start_three(one, two, three, kind)
    t = VM.new(@opts.merge(:port => one, :tag => "master", :trace => false))
    t2 = start_kind(kind, two)
    t3 = start_kind(kind, three)

    t.add_member <+ [[1, "localhost:#{two.to_s}"]]
    t.add_member <+ [[2, "localhost:#{three.to_s}"]]

    t.run_bg
    t2.run_bg
    t3.run_bg

    return [t, t2, t3]
  end

  def test_votingpair
    t, t2, t3 = start_three(12346, 12347, 12348, "VA")

    t.sync_do{ t.begin_vote <+ [[1, 'me for king']] }
    t2.sync_do
    t2.sync_do{ assert([[1,'me for king', 'localhost:12346'], [1,'me for king', '127.0.0.1:12346']].include? t2.waiting_ballots.first) }
    t3.sync_do{ assert([[1,'me for king', 'localhost:12346'], [1,'me for king', '127.0.0.1:12346']].include? t3.waiting_ballots.first) }
    t.sync_do{ assert_equal([1, 'yes', 2, [nil, nil]], t.vote_cnt.first) }
    t.sync_do{ assert_equal([1, 'me for king', 'yes', [nil, nil]], t.vote_status.first) }

    t.stop
    t2.stop
    t3.stop
  end

  def test_votingpair2
    t, t2, t3 = start_three(12316, 12317, 12318, "VA2")

    t.sync_do{ t.begin_vote <+ [[1, 'me for king']] }
    t2.sync_do
    t2.sync_do{ assert([[1,'me for king', 'localhost:12316'], [1,'me for king', '127.0.0.1:12316']].include? t2.waiting_ballots.first) }
    t3.sync_do{ assert([[1,'me for king', 'localhost:12316'], [1,'me for king', '127.0.0.1:12316']].include? t3.waiting_ballots.first) }
    t2.sync_do{ t2.cast_vote <+ [[1, "hell yes", "sir"]] }
    t.sync_do{}

    t.sync_do{ assert_equal([1, 'hell yes', 1, ["sir"]], t.vote_cnt.first) }
    t.sync_do{ assert_equal([1, 'me for king', 'in flight', nil], t.vote_status.first) }
    t3.sync_do{ t3.cast_vote <+ [[1, "hell yes", "madam"]] }
    t.sync_do

    t.sync_do{ assert_equal([1, 'hell yes', 2, ["madam", "sir"]], sort_contents(t.vote_cnt.first))}
    t.sync_do{ assert_equal([1, 'me for king', 'hell yes', ["madam", "sir"]], sort_contents(t.vote_status.first)) }
    t.stop
    t2.stop
    t3.stop
  end
  def sort_contents(tuple)
    tuple[-1].sort! # In both cote_cnt and vote_status, "contents" is the last field.
    tuple
  end
end
