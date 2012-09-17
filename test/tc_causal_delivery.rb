require './test_common'
require 'delivery/causal'

class CausalAgent
  include Bud
  include CausalDelivery
end

class TestCausalDelivery < MiniTest::Unit::TestCase
  def register_recv_cb(bud, q)
    bud.register_callback(:pipe_out) do |t|
      raise unless t.length == 1
      q.push(t.first.ident)
    end
  end

  def test_basic
    agents = (1..3).map { CausalAgent.new }
    agents.each {|a| a.run_bg}

    a, b, c = agents
    q_b = Queue.new
    q_c = Queue.new
    register_recv_cb(b, q_b)
    register_recv_cb(c, q_c)

    a.sync_do {
      a.pipe_in <+ [[b.ip_port, a.ip_port, 1, "foo1"]]
    }
    ident = q_b.pop
    assert_equal(1, ident)

    a.sync_do {
      a.pipe_in <+ [[b.ip_port, a.ip_port, 2, "foo2"]]
    }
    ident = q_b.pop
    assert_equal(2, ident)

    a.sync_do {
      a.pipe_in <+ [[c.ip_port, a.ip_port, 3, "bar"]]
    }
    ident = q_c.pop
    assert_equal(3, ident)

    agents.each {|a| a.stop}
  end

  def test_reorder_simple
    # Drop outright messages in "chn" with payload "c" and delay all other
    # messages until we've seen a message with payload "d".
    seen_d = false
    f = lambda do |tbl_name, tups|
      return [tups, []] unless tbl_name == :chn

      # Accept all non-"c" messages if we've already seen "d"
      tups = tups.reject {|t| t[3] == "c"}
      return [tups, []] if seen_d

      accepted = []
      postponed = []
      tups.each do |t|
        payload = t[3]
        if payload == "d"
          seen_d = true
          accepted << t
        else
          postponed << t
        end
      end

      return [accepted, postponed]
    end

    src = CausalAgent.new
    dst = CausalAgent.new(:channel_filter => f)
    agents = [src, dst]
    agents.each {|a| a.run_bg}

    chn_q = Queue.new
    # Stash the payloads associated with all received messages at the recipient
    # node into chn_q; this is ALL received messages, not just causal ones. Note
    # that this does _not_ include messages that are filtered or delayed by the
    # channel filter.
    dst.register_callback(:chn) do |t|
      t.each {|v| chn_q.push(v[3]) }
    end
    pipe_q = Queue.new
    # pipe_q contains the payloads of causally-delivered messages
    dst.register_callback(:pipe_out) do |t|
      t.each {|v| pipe_q.push(v[3]) }
    end

    src.sync_do {
      src.pipe_in <+ [[dst.ip_port, src.ip_port, 1, "a"]]
    }
    src.sync_do {
      src.pipe_in <+ [[dst.ip_port, src.ip_port, 2, "b"]]
    }
    src.sync_do {
      src.pipe_in <+ [[dst.ip_port, src.ip_port, 3, "c"]]
    }
    assert(chn_q.empty?)
    assert(pipe_q.empty?)

    src.sync_do {
      src.pipe_in <+ [[dst.ip_port, src.ip_port, 4, "d"]]
    }

    assert_equal("d", chn_q.pop)
    assert(pipe_q.empty?)

    src.sync_do {
      src.pipe_in <+ [[dst.ip_port, src.ip_port, 5, "e"]]
    }

    rest_chn = [chn_q.pop, chn_q.pop, chn_q.pop]
    assert_equal(["a", "b", "e"], rest_chn.sort)

    assert_equal("a", pipe_q.pop)
    dst.sync_do
    assert_equal("b", pipe_q.pop)
    dst.sync_do
    assert(pipe_q.empty?)
    assert(chn_q.empty?)
    # "d" or "e" should never appear in pipe_q, because message "c" was dropped

    agents.each {|a| a.stop}
  end
end
