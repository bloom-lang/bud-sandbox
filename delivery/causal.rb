require "rubygems"
require "bud"
require "delivery/delivery"

# XXX: compose with reliable delivery
module CausalDelivery
  include DeliveryProtocol

  state do
    channel :chn, [:@dst, :src, :ident] => [:payload, :clock]
    table :recv_buf, chn.schema
    scratch :buf_chosen, recv_buf.schema

    lat_map :my_vc
    lat_map :next_vc
  end

  bootstrap do
    my_vc <= [[ip_port, MaxLattice.wrap(0)]]
  end

  bloom :vc_update do
    # If there are any incoming or outgoing messages, bump the local VC; merge
    # local VC with VCs of incoming messages
    next_vc <= my_vc
    next_vc <= pipe_in { [ip_port, my_vc[ip_port] + 1]}
    next_vc <= chn { [ip_port, my_vc[ip_port] + 1]}
    next_vc <= chn {|c| c.clock}
    my_vc <+ next_vc
  end

  bloom :msg_io do
    chn <~ pipe_in {|m| [m.dst, m.src, m.ident, m.payload, next_vc]}
    pipe_out <= pipe_in         # Report success immediately: unreliable delivery
    recv_buf <= chn
  end

  bloom :causal_pred do
    # Deliver all messages that causally precede ("happen before") our local
    # vector clock
    buf_chosen <= recv_buf {|b| b if b.clock < next_vc}
    recv_buf <- buf_chosen
    pipe_sent <= buf_chosen
  end
end
