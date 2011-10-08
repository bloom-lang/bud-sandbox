require 'rubygems'
require 'bud'

module DeliveryProtocol
  state do
    # At the sender side, used to request that a new message be delivered. The
    # recipient address is given by the "dst" field.
    interface input, :pipe_in, [:dst, :src, :ident] => [:payload]

    # At the sender side, the transport protocol will insert a corresponding
    # "pipe_sent" fact when a message has been delivered.
    interface output, :pipe_sent, [:dst, :src, :ident] => [:payload]

    # At the recipient side, this indicates that a new message has been delivered.
    interface output, :pipe_out, [:dst, :src, :ident] => [:payload]
  end
end

module BestEffortDelivery
  include DeliveryProtocol

  state do
    channel :pipe_chan, [:@dst, :src, :ident] => [:payload]
  end

  bloom :snd do
    pipe_chan <~ pipe_in
  end

  bloom :rcv do
    pipe_out <= pipe_chan
  end

  bloom :done do
    # Report success immediately -- this implementation of "best effort" is more
    # like "an effort".
    pipe_sent <= pipe_in
  end
end
