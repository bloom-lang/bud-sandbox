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

  bloom :rcv do
    ack <~ bed.pipe_chan.map {|p| [p.src, p.dst, p.ident]}
  end

  bloom :done do
    msg_done = (ack * buf).rights(:ident => :ident)
    pipe_sent <= msg_done
    buf <- msg_done
  end
end
