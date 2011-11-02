require 'rubygems'
require 'bud'
require 'delivery/delivery'

# At the sender side, we assign monotonically-increasing IDs to outgoing
# messages. Senders buffer messages until they receive an acknowledgment from
# the recipient that a particular ID range has been received.
#
# At the recipient side, we deliver messages to the application when we've
# received all preceding messages in the ID sequence. We send acknowledgments
# back to the sender with the lowest contiguous ID sequence point every second
# or so; the sender can use this both to garbage collect the send buffer and
# retransmit lost messages.
#
# Since all messages sent in the same timestep are logically concurrent, we
# _could_ assign them the same ID value -- but then we'd need another mechanism
# to ensure that all tuples with a given ID value have been delivered.
module OrderedDelivery
  include DeliveryProtocol

  state do
    channel :ord_chan, [:@dst, :src, :seq] => [:ident, :payload]

    # Sender state
    table :send_seq, [] => [:val]
    scratch :pipe_in_ord, ord_chan.schema
    scratch :to_send, ord_chan.schema
    table :send_buf, ord_chan.schema

    # Receiver state
    table :recv_buf, ord_chan.schema
    table :recv_deliver_seq, [] => [:val]
    scratch :to_deliver, ord_chan.schema
    periodic :recv_tik, 1
  end

  bootstrap do
    send_seq <= [[0]]
    recv_deliver_seq <= [[0]]
  end

  bloom :snd do
    pipe_in_ord <= pipe_in.sort.each_with_index.map do |m, i|
      [m.dst, m.src, i, m.ident, m.payload]
    end
    to_send <= (pipe_in_ord * send_seq).pairs do |m, i|
      [m.dst, m.src, m.seq + i, m.ident, m.payload]
    end
    ord_chan <~ to_send
    send_buf <= to_send
  end

  bloom :recv do
    # Don't bother trying to buffer messages that we've already delivered to the
    # client application
    recv_buf <= (ord_chan * recv_deliver_seq).pairs do |m, s|
      m if m.seq > s.val
    end

    # Check if we can legally deliver any messages in the buffer
    to_deliver <= (recv_buf * recv_deliver_seq).pairs do |m, s|
      m if m.seq == (s.val + 1)
    end
    recv_buf <- to_deliver
    recv_deliver_seq <+- to_deliver {|m| m.seq}
    pipe_out <= to_deliver {|m| [m.dst, m.src, m.ident, m.payload]}
  end
end
