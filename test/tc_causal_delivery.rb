require 'rubygems'
require 'bud'
require 'test/unit'
require 'delivery/causal'

class CausalAgent
  include Bud
  include CausalDelivery
end

class TestCausalDelivery < Test::Unit::TestCase
  def test_basic
    agents = (1..3).map { CausalAgent.new }
    agents.each {|a| a.run_bg}

    a, b, c = agents
    a.sync_do {
      a.pipe_in <+ [[b.ip_port, a.ip_port, 1, "foo"]]
    }
    a.sync_do {
      a.pipe_in <+ [[c.ip_port, a.ip_port, 2, "bar"]]
    }

    sleep 3
    agents.each {|a| a.stop}
  end
end
