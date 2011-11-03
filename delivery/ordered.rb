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
    channel :ack_chan, [:@dst, :src, :seq]

    # Sender state
    table :send_seq, [] => [:val]
    scratch :pipe_in_ord, ord_chan.schema
    scratch :to_send, ord_chan.schema
    table :send_buf, ord_chan.schema

    # Receiver state
    table :recv_buf, ord_chan.schema
    table :recv_deliver_seq, [:sender] => [:val]
    scratch :new_sender_msg, ord_chan.schema
    scratch :to_deliver, ord_chan.schema
    periodic :recv_tik, 1
  end

  bootstrap do
    send_seq <= [[0]]
  end

  bloom :snd do
    pipe_in_ord <= pipe_in.sort.each_with_index.map do |m, i|
      [m.dst, m.src, i, m.ident, m.payload]
    end
    to_send <= (pipe_in_ord * send_seq).pairs do |m, i|
      [m.dst, m.src, m.seq + i.val + 1, m.ident, m.payload]
    end
    ord_chan <~ to_send
    send_buf <= to_send
    stdio <~ to_send {|m| ["To send @ #{port}: #{m.inspect}"]}
  end

  bloom :on_ack do
    stdio <~ ack_chan {|m| ["Got ack @ #{port}: #{m.inspect}"]}

    # Remove all messages from the buffer that the recipient has acknowledged
    # and report successful delivery to the client application
    send_buf <- (send_buf * ack_chan).pairs(:dst => :src) do |m, a|
      m if m.seq <= a.seq
    end
    pipe_sent <= (send_buf * ack_chan).pairs(:dst => :src) do |m, a|
      [m.dst, m.src, m.ident, m.payload] if m.seq <= a.seq
    end

    # Resend all pending messages not contained in the acknowledgment
    # XXX: this means resending far too much if the recipient has only missed a
    # single message
    ord_chan <~ (send_buf * ack_chan).pairs(:dst => :src) do |m, a|
      m if m.seq > a.seq
    end
  end

  bloom :recv do
    stdio <~ ord_chan {|m| ["Received @ #{port}: #{m.inspect}"]}

    # Don't bother trying to buffer messages that have already been delivered to
    # the client application
    recv_buf <= (ord_chan * recv_deliver_seq).pairs(:src => :sender) do |m, s|
      m if m.seq > s.val
    end

    # If this is the first message we've seen from this sender, set the sequence
    # number low water mark to 0
    # XXX: no reason for this to really be non-monotonic
    new_sender_msg <= ord_chan.notin(recv_deliver_seq, :src => :sender)
    recv_deliver_seq <+ new_sender_msg {|m| [m.src, 0]}
    recv_buf <= new_sender_msg

    # Check if we can legally deliver any messages in the buffer
    to_deliver <= (recv_buf * recv_deliver_seq).pairs(:src => :sender) do |m, s|
      m if m.seq == (s.val + 1)
    end
    recv_buf <- to_deliver
    recv_deliver_seq <+- (to_deliver * recv_deliver_seq).lefts(:src => :sender) do |m|
      [m.src, m.seq]
    end
    pipe_out <= to_deliver {|m| [m.dst, m.src, m.ident, m.payload]}

    # Periodically inform each sender of the greatest-delivered sequence number
    # at this site
    ack_chan <~ (recv_tik * recv_deliver_seq).rights do |s|
      [s.sender, ip_port, s.val]
    end
  end
end
