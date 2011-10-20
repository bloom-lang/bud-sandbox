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
end
