require 'rubygems'
require 'test/unit'
require 'bud'
require 'delivery/dastardly_delivery'

#this is basically a copy of the reliable_delivery test class
#this isn't pretty, but performs the correct test
class DastardlyD
  include Bud
  import DastardlyDelivery => :dd

  state do
    #store the time at which the messages come in to make sure we're reordering them
    table :pipe_chan_perm, [:msg, :t] => []
    table :pipe_sent_perm, dd.pipe_sent.schema
    scratch :got_pipe, dd.pipe_chan.schema

    # XXX: only necessary because we don't rewrite sync_do blocks
    scratch :send_msg, dd.pipe_in.schema

    scratch :set_max_delay_wrap, dd.max_delay.schema
  end

  bloom do
    dd.set_max_delay <= set_max_delay_wrap
    pipe_sent_perm <= dd.pipe_sent
    pipe_chan_perm <= dd.pipe_chan { |m| [m, @budtime] }
    got_pipe <= dd.pipe_chan
    dd.pipe_in <= send_msg
  end
end

class TestDastardlyDelivery < Test::Unit::TestCase
  def test_dd_delivery_reorder
    snd = DastardlyD.new
    rcv = DastardlyD.new
    
    srand(0)

    snd.run_bg
    rcv.run_bg

    snd.sync_do {
      snd.set_max_delay_wrap <+ [[2]]
    }
    snd.sync_do

    q = Queue.new
    rcv.register_callback(:got_pipe) do
      q.push(true)
    end

    values = [[1, 'foo'], [2, 'bar'], [3, 'baz'], [4,'qux']]
    tuples = values.map {|v| [rcv.ip_port, snd.ip_port] + v}

    tuples.each do |t|
      snd.sync_do {
        snd.send_msg <+ [t]
      }
    end

    # Wait for messages to be delivered to rcv
    tuples.length.times { q.pop }

    rcv.sync_do {
      #note that the message delivery order (due to the sockets?) changes
      #from 2,3,4,1 to 2,1,3,4 (at least on my machine) so we just make
      #sure the orders aren't equal instead of checking for any particular
      #order--pdb
      assert_not_equal(tuples,
                       rcv.pipe_chan_perm.to_a.sort { |a, b| a[1] <=> b[1] }.map { |m| m[0] } )
    }
    snd.stop_bg
    rcv.stop_bg
  end

  def test_dd_delivery_send_once
    snd = DastardlyD.new
    rcv = DastardlyD.new
    snd.run_bg
    rcv.run_bg

    snd.sync_do {
      snd.set_max_delay_wrap <+ [[1]]
    }
    snd.sync_do

    q = Queue.new
    rcv.register_callback(:got_pipe) do
      q.push(true)
    end

    values = [[1, 'foo']]
    tuples = values.map {|v| [rcv.ip_port, snd.ip_port] + v}

    tuples.each do |t|
      snd.sync_do {
        snd.send_msg <+ [t]
      }
    end

    # Wait for message to be delivered to rcv
    tuples.length.times  do
      q.pop
    end

    rcv.sync_do {
      assert_equal(tuples[0],
                   rcv.pipe_chan_perm.to_a[0][0])
    }
    snd.stop_bg
    rcv.stop_bg
  end
end
