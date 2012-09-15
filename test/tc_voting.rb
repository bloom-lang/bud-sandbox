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
    kind.new(@opts.merge(:port => port, :tag => port, :trace => false))
  end

  def start_three(one, two, three, kind)
    t = VM.new(@opts.merge(:port => one, :tag => "master", :trace => false))
    t2 = start_kind(kind, two)
    t3 = start_kind(kind, three)

    t.add_member <+ [[1, "localhost:#{two}"]]
    t.add_member <+ [[2, "localhost:#{three}"]]

    t.run_bg
    t2.run_bg
    t3.run_bg

    return [t, t2, t3]
  end

  def simple_cb(bud_i, tbl_name)
    q = Queue.new
    cb = bud_i.register_callback(tbl_name) do
      q.push(true)
    end
    [q, cb]
  end

  def block_for_cb(bud_i, q, cb, unregister=true)
    q.pop
    bud_i.unregister_callback(cb) if unregister
  end

  def test_votingpair
    t, t2, t3 = start_three(12346, 12347, 12348, VA)

    t_q, t_cb = simple_cb(t, :vote)
    t2_q, t2_cb = simple_cb(t2, :ballot)
    t3_q, t3_cb = simple_cb(t3, :ballot)

    t.sync_do{ t.begin_vote <+ [[1, 'me for king']] }
    block_for_cb(t2, t2_q, t2_cb)
    block_for_cb(t3, t3_q, t3_cb)
    t2.sync_do{ assert_equal([1, 'me for king', t.ip_port], t2.waiting_ballots.first) }
    t3.sync_do{ assert_equal([1, 'me for king', t.ip_port], t3.waiting_ballots.first) }

    block_for_cb(t, t_q, t_cb)
    t.sync_do{ assert_equal([1, 'yes', 2, [nil].to_set], t.vote_cnt.first) }
    t.sync_do{ assert_equal([1, 'me for king', 'yes', [nil].to_set], t.vote_status.first) }

    t.stop
    t2.stop
    t3.stop
  end

  def test_votingpair2
    t, t2, t3 = start_three(12316, 12317, 12318, VA2)

    t_q, t_cb = simple_cb(t, :vote)
    t2_q, t2_cb = simple_cb(t2, :ballot)
    t3_q, t3_cb = simple_cb(t3, :ballot)

    t.sync_do{ t.begin_vote <+ [[1, 'me for king']] }
    block_for_cb(t2, t2_q, t2_cb)
    block_for_cb(t3, t3_q, t3_cb)
    t2.sync_do{ assert_equal([1, 'me for king', t.ip_port], t2.waiting_ballots.first) }
    t3.sync_do{ assert_equal([1, 'me for king', t.ip_port], t3.waiting_ballots.first) }

    t2.sync_do{ t2.cast_vote <+ [[1, "hell yes", "sir"]] }
    block_for_cb(t, t_q, t_cb, false)
    t.sync_do{ assert_equal([1, 'hell yes', 1, ["sir"].to_set], t.vote_cnt.first) }
    t.sync_do{ assert_equal([1, 'me for king', 'in flight', nil], t.vote_status.first) }

    t3.sync_do{ t3.cast_vote <+ [[1, "hell yes", "madam"]] }
    block_for_cb(t, t_q, t_cb)
    t.sync_do{ assert_equal([1, 'hell yes', 2, ["madam", "sir"].to_set], t.vote_cnt.first)}
    t.sync_do{ assert_equal([1, 'me for king', 'hell yes', ["madam", "sir"].to_set], t.vote_status.first) }

    t.stop
    t2.stop
    t3.stop
  end
end
