require 'rubygems'
require 'bud'

module DeliveryProtocol
  include BudModule

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
    # vacuous ackuous.  override me!
    pipe_sent <= pipe_in
  end
end
