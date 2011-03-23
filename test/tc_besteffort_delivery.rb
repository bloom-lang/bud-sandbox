require 'rubygems'
require 'test/unit'
require 'bud'
require 'delivery/delivery'

class BED
  include Bud
  import BestEffortDelivery => :bed

  state do
    table :pipe_chan_perm, bed.pipe_chan.schema
    table :pipe_sent_perm, bed.pipe_sent.schema
    scratch :got_pipe, bed.pipe_chan.schema

    # XXX: only necessary because we don't rewrite sync_do blocks
    scratch :send_msg, bed.pipe_in.schema
  end

  bloom do
    pipe_sent_perm <= bed.pipe_sent
    pipe_chan_perm <= bed.pipe_chan
    got_pipe <= bed.pipe_chan
    bed.pipe_in <= send_msg
  end
end

class TestBEDelivery < Test::Unit::TestCase
  # XXX: broken
  def broken_test_besteffort_delivery
    rd = BED.new
    sendtup = ['localhost:11116', 'localhost:11115', 1, 'foobar']
    rd.run_bg

    return
    rd.sync_do{ rd.pipe_in <+ [ sendtup ] }
    sleep 1
    rd.sync_do {
      assert_equal(1, rd.pipe_sent_perm.length)
      assert_equal(sendtup, rd.pipe_sent_perm.first)
    }
    rd.stop_bg
  end

  def test_bed_delivery
    snd = BED.new
    rcv = BED.new
    snd.run_bg
    rcv.run_bg

    q = Queue.new
    rcv.register_callback(:got_pipe) do
      q.push(true)
    end

    values = [[1, 'foo'], [2, 'bar']]
    tuples = values.map {|v| [rcv.ip_port, snd.ip_port] + v}

    tuples.each do |t|
      snd.sync_do {
        snd.send_msg <+ [t]
      }
    end

    # Wait for messages to be delivered to rcv
    tuples.length.times { q.pop }

    rcv.sync_do {
      assert_equal(tuples.sort, rcv.pipe_chan_perm.to_a.sort)
    }
    snd.stop_bg
    rcv.stop_bg
  end
end
