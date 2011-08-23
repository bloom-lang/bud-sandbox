require 'rubygems'
require 'bud'
require 'delivery/delivery'

module UnreliableDeliveryControl
  state do
    #percentage chance of message loss, 0 to 100
    interface input, :set_drop_pct, [] => [:pct]
  end
end

module UnreliableDelivery
  include DeliveryProtocol
  include UnreliableDeliveryControl

  state do
    table :drop_pct, [] => [:pct]
    channel :pipe_chan, [:@dst, :src, :ident] => [:payload]
  end

  bootstrap do
    drop_pct <= [[50]]
  end

  bloom :control do
    #this is a hack and should be replaced with <+- once supported
    drop_pct <- (drop_pct * set_drop_pct).pairs { |o, n| o }
    drop_pct <+ set_drop_pct
  end

  bloom :snd do
    pipe_chan <~ (pipe_in * drop_pct).pairs do |i, p|
      if p.pct <= rand(100)
        i
      end
    end
  end

  bloom :done do
    pipe_sent <= pipe_in
  end
end
