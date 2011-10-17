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
    b = CausalAgent.new
  end
end
