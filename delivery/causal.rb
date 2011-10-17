require "rubygems"
require "bud"
require "delivery/delivery"

module CausalDelivery
  include DeliveryProtocol

  state do
    channel :chn, [:@dst, :src, :ident] => [:payload, :clock]

    # Sender-side state

    # Recipient-side state
  end
end
