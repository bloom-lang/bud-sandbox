require 'rubygems'
require 'test/unit'
require 'bud'
require 'delivery/demonic_delivery'

#this is basically a copy of the reliable_delivery test class
#this isn't pretty, but performs the correct test
class DemonD
  include Bud
  import DemonicDelivery => :dd

  state do
    table :pipe_out_perm, dd.pipe_out.schema
    table :pipe_sent_perm, dd.pipe_sent.schema
    scratch :got_pipe, dd.pipe_out.schema

    # XXX: only necessary because we don't rewrite sync_do blocks
    scratch :send_msg, dd.pipe_in.schema

    scratch :set_drop_pct_wrap, dd.drop_pct.schema
  end

  bloom do
    dd.set_drop_pct <= set_drop_pct_wrap
    pipe_sent_perm <= dd.pipe_sent
    pipe_out_perm <= dd.pipe_out
    got_pipe <= dd.pipe_out
    dd.pipe_in <= send_msg
  end
end

class TestDemonicDelivery < Test::Unit::TestCase
  def test_dd_delivery_reliable
    snd = DemonD.new
    rcv = DemonD.new
    snd.run_bg
    rcv.run_bg

    snd.sync_do {
      snd.set_drop_pct_wrap <+ [[0]]
    }
    snd.sync_do

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
      assert_equal(tuples.sort, rcv.pipe_out_perm.to_a.sort)
    }
    snd.stop_bg
    rcv.stop_bg
  end

  def test_dd_delivery_demonic
    srand(0)
    snd = DemonD.new
    rcv = DemonD.new
    snd.run_bg
    rcv.run_bg

    snd.sync_do {
      snd.set_drop_pct_wrap <+ [[50]]
    }
    snd.sync_do

    q = Queue.new
    rcv.register_callback(:got_pipe) do
      q.push(true)
    end

    values = [[1, 'foo'], [2, 'bar'], [3, 'baz'], [4, 'qux'], [5,'quux']]
    tuples = values.map {|v| [rcv.ip_port, snd.ip_port] + v}

    tuples.each do |t|
      snd.sync_do {
        snd.send_msg <+ [t]
      }
    end

    # Under this seed, expect only messages 3-5
    # Wait for messages to be delivered to rcv
    (3.times { q.pop })

    rcv.sync_do

    rcv.sync_do {
      assert_equal(tuples.sort.slice(2, tuples.length),
                   rcv.pipe_out_perm.to_a.sort)
    }
    snd.stop_bg
    rcv.stop_bg
  end
end
