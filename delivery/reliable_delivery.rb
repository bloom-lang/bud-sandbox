require 'rubygems'
require 'bud'
require 'delivery/delivery'

module ReliableDelivery
  include BestEffortDelivery

  state {
    table :buf, [:dst, :src, :ident] => [:payload]
    channel :ack, [:@src, :dst, :ident]
    periodic :clock, 2
  }

  declare
  def remember
    buf <= pipe_in
    pipe_chan <~ join([buf, clock]).map {|b, c| b}
  end

  declare
  def rcv
    ack <~ pipe_chan.map {|p| [p.src, p.dst, p.ident]}
  end

  declare
  def done
    got_ack = join [ack, buf], [ack.ident, buf.ident]
    msg_done = got_ack.map {|a, b| b}

    pipe_sent <= msg_done
    buf <- msg_done
  end
end
