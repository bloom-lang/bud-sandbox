require 'delivery/delivery'

#intentionally drops messages, but reports success

module DemonicDeliveryControl
  state do
    #percentage chance of message loss, 0 to 100
    interface input, :set_drop_pct, [] => [:pct]
  end
end

module DemonicDelivery
  include DeliveryProtocol
  include DemonicDeliveryControl

  state do
    table :drop_pct, [] => [:pct]
    channel :pipe_chan, [:@dst, :src, :ident] => [:payload]
  end

  bootstrap do
    drop_pct <= [[50]]
  end

  bloom :control do
    drop_pct <+- set_drop_pct
  end

  bloom :snd do
    pipe_chan <~ (pipe_in * drop_pct).pairs do |i, p|
      if p.pct <= rand(100)
        i
      end
    end
  end

  bloom :rcv do
    pipe_out <= pipe_chan
  end

  bloom :done do
    pipe_sent <= pipe_in
  end
end
