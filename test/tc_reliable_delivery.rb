require 'rubygems'
require 'bud'
require 'test/unit'
require 'delivery/reliable'
require 'enumerator'

class RED
  include Bud
  import ReliableDelivery => :rd

  state do
    table :recv_log, rd.pipe_out.schema
    scratch :msg_sent, rd.pipe_sent.schema
  end

  bloom do
    recv_log <= rd.pipe_out
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
    tuples = vals.each_with_index.map do |v, i|
      [rd2.ip_port, rd.ip_port, i, v]
    end

    rd.send_msg(tuples)
    tuples.length.times { q.pop }
    rd2.sync_do { assert_equal(tuples, rd2.recv_log.to_a.sort) }

    # Advance to the next tick so that the final delivered tuple is deleted from
    # the sender's buffer
    rd.sync_do
    assert(rd.buf_empty?)

    rd.stop
    rd2.stop
  end

  def test_not_delivered
    rd = RED.new
    rd.run_bg

    sendtup = ['localhost:999', rd.ip_port, 1, 'foobar']
    rd.send_msg([sendtup])

    # transmission not 'complete'
    #assert_equal(false, rd.buf_empty?) # buf is emptied lazily, so this assertion will not work anymore
    rd.sync_do { assert_equal([], rd.recv_log.to_a.sort) }
    rd.stop
  end
end
