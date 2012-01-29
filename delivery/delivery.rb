# @abstract DeliveryProtocol is the abstract interface for message delivery.
# A delivery implementation should subclass DeliveryProtocol.
module DeliveryProtocol
  state do
    # At the sender side, used to request that a new message be delivered. The
    # recipient address is given by the "dst" field.
    # @param [String] dst the destination address
    # @param [String] src the sending address
    # @param [Number] ident a unique id for the message
    # @param [Object] payload is the message payload
    # @return [pipe_sent] when the tuple we inserted appears in :pipe_sent, delivery is successful
    interface input, :pipe_in, [:dst, :src, :ident] => [:payload]

    # At the sender side, the transport protocol will insert a corresponding
    # "pipe_sent" fact when a message has been delivered.
    # @param [String] dst the destination address
    # @param [String] src the sending address
    # @param [Number] ident a unique id for the message
    # @param [Object] payload is the message payload
    interface output, :pipe_sent, [:dst, :src, :ident] => [:payload]

    # At the recipient side, this indicates that a new message has been delivered.
    # @param [String] dst the destination address
    # @param [String] src the sending address
    # @param [Number] ident a unique id for the message
    # @param [Object] payload is the message payload
    interface output, :pipe_out, [:dst, :src, :ident] => [:payload]
  end

end

# BestEffortDelivery is the simplest imaginable implementation of DeliveryProtocol.
# It makes 'an effort' to deliver a message inserted into :pipe_in.
# @see DeliveryProtocol implements DeliveryProtocol
module BestEffortDelivery
  include DeliveryProtocol

  state do
    # internal channel used for message transmission
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
