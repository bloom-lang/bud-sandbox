require 'rubygems'
require 'bud'
require 'test/unit'
require 'delivery/reliable_delivery'
require 'enumerator'

class RED
  include Bud
  import ReliableDelivery => :rd

  state do
    table :pipe_log, rd.pipe_sent.schema
    scratch :msg_sent, rd.pipe_sent.schema
  end

  bloom do
    pipe_log <= rd.pipe_sent
    msg_sent <= rd.pipe_sent
  end

  def send_msg(t)
    sync_do {
      rd.pipe_in <+ t
    }
  end

  def buf_empty?
    rv = nil
    sync_do {
      rv = rd.buf.empty?
    }
    return rv
  end
end

class TestReliableDelivery < Test::Unit::TestCase
  def test_rdelivery
    rd = RED.new
    rd2 = RED.new
    rd.run_bg
    rd2.run_bg

    q = Queue.new
    rd.register_callback(:msg_sent) do
      q.push(true)
    end

    vals = ("aa".."dd").to_a
    tuples = vals.enum_with_index.map do |v, i|
      [rd2.ip_port, rd.ip_port, i, v]
    end

    rd.send_msg(tuples)
    tuples.length.times { q.pop }
    rd.sync_do { assert_equal(tuples, rd.pipe_log.to_a.sort) }
    assert(rd.buf_empty?)

    rd.stop_bg
    rd2.stop_bg
  end

  def test_not_delivered
    rd = RED.new
    rd.run_bg

    sendtup = ['localhost:999', rd.ip_port, 1, 'foobar']
    rd.send_msg([sendtup])

    # transmission not 'complete'
    assert_equal(false, rd.buf_empty?)
    rd.sync_do { assert_equal([], rd.pipe_log.to_a.sort) }
    rd.stop_bg
  end
end
