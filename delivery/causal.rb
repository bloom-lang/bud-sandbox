require "rubygems"
require "bud"
require "delivery/delivery"

# A protocol for point-to-point causal delivery, based on "Schiper, A., Eggli,
# J., & Sandoz, A. (1989). A New Algorithm To Implement Causal Ordering.
# International Workshop on Distributed Algorithms."
#
# XXX: compose with reliable delivery
module CausalDelivery
  include DeliveryProtocol

  state do
    channel :chn, [:@dst, :src, :ident] => [:payload, :clock, :ord_buf]

    # Local vector clock
    lat_map :my_vc
    lat_map :next_vc

    # Our knowledge of the VCs at other nodes
    lat_map :ord_buf

    # Received messages that haven't yet been delivered
    table :recv_buf, chn.schema
    scratch :buf_chosen, recv_buf.schema
  end

  bootstrap do
    my_vc <= [[ip_port, MaxLattice.wrap(0)]]
  end

  bloom :update_vc do
    next_vc <= my_vc
    next_vc <= pipe_in { [ip_port, my_vc[ip_port] + 1]}
    next_vc <= buf_chosen { [ip_port, my_vc[ip_port] + 1]}
    next_vc <= buf_chosen {|m| m.clock}
    my_vc <+ next_vc
  end

  bloom :outbound_msg do
    chn <~ pipe_in {|p| [p.dst, p.src, p.ident, p.payload, next_vc, ord_buf]}
    ord_buf <+ pipe_in {|p| [p.dst, next_vc]}
    # Report success immediately
    # XXX: unreliable delivery is problematic
    pipe_out <= pipe_in
  end

  bloom :inbound_msg do
    stdio <~ chn {|c| ["(#{@budtime}) Inbound message @ #{port}: #{[c.src, c.ident, c.payload].inspect}, msg VC = #{c.clock.inspected}, msg ord_buf = #{c.ord_buf.inspected}, local VC: #{my_vc.inspected}, local ord_buf: #{ord_buf.inspected}"]}
    stdio <~ pipe_sent {|m| ["(#{@budtime}) Delivering message @ #{port}: #{m.ident}"]}

    recv_buf <= chn
    buf_chosen <= recv_buf {|m| m if m.ord_buf[ip_port].lt_eq(my_vc)}
    recv_buf <- buf_chosen

    pipe_sent <= buf_chosen {|m| [m.dst, m.src, m.ident, m.payload]}
    ord_buf <+ buf_chosen {|m| m.ord_buf}
  end
end
