require 'rubygems'
require 'bud'
require 'delivery/delivery'

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

  bloom :send_ack do
    ack <~ bed.pipe_chan {|p| [p.src, p.dst, p.ident]}
  end

  bloom :done do
    temp :msg_acked <= (buf * ack).lefts(:ident => :ident)
    pipe_sent <= msg_acked
    buf <- msg_acked
  end
end
