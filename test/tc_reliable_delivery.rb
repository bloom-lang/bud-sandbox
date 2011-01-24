require 'rubygems'
require 'bud'
require 'test/unit'
require 'delivery/reliable_delivery'

class RED < Bud
  include ReliableDelivery
  
  def state
    super 
    table :pipe_perm, ['dst', 'src', 'ident', 'payload']
  end
  
  declare 
  def recall
    pipe_perm <= pipe_sent 
  end
end

class TestBEDelivery < Test::Unit::TestCase
  def test_delivery1
    rd = RED.new("localhost", 12222, {})
    rd.run_bg

    sendtup = ['localhost:12223', 'localhost:12222', 1, 'foobar']
    rd.pipe_in <+ [ sendtup ]

    # transmission not 'complete'
    assert_equal(0, rd.pipe_perm.length)
  end


  def test_besteffort_delivery2
    rd = RED.new("localhost", 13333, {})
    rd2 = RED.new("localhost", 13334, {})
    rd.run_bg
    rd2.run_bg
    #sleep 1
    sendtup = ['localhost:13334', 'localhost:13333', 1, 'foobar']
    rd.pipe_in <+ [ sendtup ]

    # debugging
    #assert_equal(1, rd.pipe.length)
  
    sleep 6

    # transmission 'complete'
    assert_equal(1, rd.pipe_perm.length)

    # gc done
    assert_equal(0, rd.pipe.length)
  end


end
