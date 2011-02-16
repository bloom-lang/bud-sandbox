require 'rubygems'
require 'bud'

module DeliveryProtocol
  include BudModule

  state {
    interface input, :pipe_in, [:dst, :src, :ident] => [:payload]
    interface output, :pipe_sent, [:dst, :src, :ident] => [:payload]
  }
end

module BestEffortDelivery
  include DeliveryProtocol

  state {
    channel :pipe_chan, [:@dst, :src, :ident] => [:payload]
  }

  declare
  def snd
    pipe_chan <~ pipe_in
  end

  declare
  def done
    # vacuous ackuous.  override me!
    pipe_sent <= pipe_in
  end
end
