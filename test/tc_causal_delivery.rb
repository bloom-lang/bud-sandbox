require 'rubygems'
require 'bud'
require 'test/unit'
require 'delivery/causal'

class CausalAgent
  include Bud
  include CausalDelivery
end

class TestCausalDelivery < Test::Unit::TestCase
  def register_sent_cb(bud, q)
    bud.register_callback(:pipe_sent) do
      q.push(true)
    end
  end

  def test_basic
    agents = (1..3).map { CausalAgent.new }
    agents.each {|a| a.run_bg}

    a, b, c = agents
    q_b = Queue.new
    q_c = Queue.new
    register_sent_cb(b, q_b)
    register_sent_cb(c, q_c)

    a.sync_do {
      a.pipe_in <+ [[b.ip_port, a.ip_port, 1, "foo1"]]
    }
    q_b.pop

    a.sync_do {
      a.pipe_in <+ [[b.ip_port, a.ip_port, 2, "foo2"]]
    }
    q_b.pop

    a.sync_do {
      a.pipe_in <+ [[c.ip_port, a.ip_port, 3, "bar"]]
    }
    q_c.pop

    agents.each {|a| a.stop}
  end

  def test_reorder_simple
    # Drop messages with payload "c", and delay all other messages until we've
    # seen a message with payload "d".
    seen_d = false
    buf = []
    f = lambda do |tbl_name, tups|
      return [tups, []] unless tbl_name == :chn

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
    dst.register_callback(:chn) do |t|
      t.each {|v| chn_q.push(v[3]) }
    end
    pipe_q = Queue.new
    dst.register_callback(:pipe_sent) do |t|
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
    assert_equal("a", pipe_q.pop)
    dst.sync_do
    assert_equal("b", pipe_q.pop)
    assert(pipe_q.empty?)

    agents.each {|a| a.stop}
  end
end
