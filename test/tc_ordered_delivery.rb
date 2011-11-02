require 'rubygems'
require 'bud'
require 'test/unit'
require 'delivery/ordered_delivery'

class OrdAgent
  include Bud
  include OrderedDelivery

  state do
    table :recv_log, pipe_out.schema
  end

  bloom do
    recv_log <= pipe_out
  end
end

class TestOrderedDelivery < Test::Unit::TestCase
  def test_basic
  end
end
