require 'rubygems'
require 'bud'
require 'test/unit'
require 'delivery/new_ordered'

class OrdAgent
  include Bud
  include OrderedDelivery

  state do
    table :recv_log, pipe_out.schema
    table :sent_log, pipe_sent.schema
  end

  bloom do
    recv_log <= pipe_out
    sent_log <= pipe_sent
  end
end

class TestOrderedDelivery < Test::Unit::TestCase
  def test_basic
    agents = (1..2).map { OrdAgent.new }
    agents.each {|a| a.run_bg}

    a, b = agents
    recv_q = Queue.new
    b.register_callback(:pipe_out) do
      recv_q.push(true)
    end
    send_q = Queue.new
    a.register_callback(:pipe_sent) do
      send_q.push(true)
    end

    a.sync_do {
      a.pipe_in <+ [[b.ip_port, a.ip_port, 1, "foo"]]
    }
    recv_q.pop

    b.sync_do {
      assert_equal([[b.ip_port, a.ip_port, 1, "foo"]], b.recv_log.to_a.sort)
    }

    send_q.pop
    a.sync_do {
      assert_equal([[b.ip_port, a.ip_port, 1, "foo"]], a.sent_log.to_a.sort)
    }

    agents.each {|a| a.stop}
  end
end
