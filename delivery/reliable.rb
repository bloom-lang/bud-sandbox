require 'rubygems'
require 'bud'
require 'delivery/delivery'

# Note that this provides at-least-once delivery. If you need exactly-once, the
# receiver-side can record the message IDs that have been received to avoid
# processing duplicate messages.
module ReliableDelivery
  include DeliveryProtocol
  import BestEffortDelivery => :bed

  state do
    table :buf, pipe_in.schema
    channel :ack, [:@src, :dst, :ident]
    periodic :clock, 2
  end

  bloom :remember do
    buf <= pipe_in
    bed.pipe_in <= pipe_in
    bed.pipe_in <= (buf * clock).lefts
  end

  bloom :rcv do
    pipe_out <= bed.pipe_out
    ack <~ bed.pipe_out {|p| [p.src, p.dst, p.ident]}
  end

  bloom :done do
    temp :msg_acked <= (buf * ack).lefts(:ident => :ident)
    pipe_sent <= msg_acked
    buf <- msg_acked
  end
end
