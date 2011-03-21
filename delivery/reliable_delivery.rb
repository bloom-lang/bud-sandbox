require 'rubygems'
require 'bud'
require 'delivery/delivery'

module ReliableDelivery
  include BestEffortDelivery

  state do
    table :buf, pipe_in.schema
    channel :ack, [:@src, :dst, :ident]
    periodic :clock, 2
  end

  bloom :remember do
    buf <= pipe_in
    pipe_chan <~ join([buf, clock]).map {|b, c| b}
  end

  bloom :rcv do
    ack <~ pipe_chan.map {|p| [p.src, p.dst, p.ident]}
  end

  bloom :done do
    got_ack = join [ack, buf], [ack.ident, buf.ident]
    msg_done = got_ack.map {|a, b| b}

    pipe_sent <= msg_done
    buf <- msg_done
  end
end
