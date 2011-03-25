require 'rubygems'
require 'bud'

module DeliveryProtocol
  state do
    interface input, :pipe_in, [:dst, :src, :ident] => [:payload]
    interface output, :pipe_sent, [:dst, :src, :ident] => [:payload]
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

  bloom :done do
    # Report success immediately -- this implementation of "best effort" is more
    # like "an effort".
    pipe_sent <= pipe_in
  end
end
