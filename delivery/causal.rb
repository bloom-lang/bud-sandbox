require "rubygems"
require "bud"
require "delivery/delivery"

# A protocol for point-to-point causal delivery, based on "Schiper, A., Eggli,
# J., & Sandoz, A. (1989). A New Algorithm To Implement Causal Ordering.
# International Workshop on Distributed Algorithms."
#
# The paper describes an optional scheme for deleting obselete ord_buf pairs: if
# site s2 gets a message m from site s1, and s2.ord_buf[s1].lt_eq(m.clock) is
# true, we can delete s2.ord_buf[s1]: we know that s1's clock is at least
# m.clock, which means it has passed the time indicated by s2.ord_buf[s1]. This
# isn't implemented, because (a) it would make ord_buf not a lattice (b) it
# seems of limited value anyway.
#
# XXX: compose with reliable delivery; otherwise causal delivery is not live.
module CausalDelivery
  include DeliveryProtocol

  state do
    channel :chn, [:@dst, :src, :ident] => [:payload, :clock, :ord_buf]

    # Local vector clock: map from node_id => lmax
    lmap :my_vc
    lmap :next_vc

    # Our knowledge of the vector clocks at other nodes: map from node_id =>
    # {map from node_id => lmax}
    lmap :ord_buf

    # Received messages that haven't yet been delivered
    table :recv_buf, chn.schema

    scratch :buf_chosen, recv_buf.schema
  end

  bootstrap do
    my_vc <= {ip_port => Bud::MaxLattice.new(0)}
  end

  bloom :update_vc do
    next_vc <= my_vc
    # On outgoing messages:
    next_vc <= pipe_in { {ip_port => my_vc.at(ip_port) + 1} }
    # On incoming messages:
    next_vc <= buf_chosen { {ip_port => my_vc.at(ip_port) + 1} }
    next_vc <= buf_chosen {|m| m.clock}
    my_vc <+ next_vc
  end

  bloom :outbound_msg do
    chn <~ pipe_in {|p| [p.dst, p.src, p.ident, p.payload, next_vc, ord_buf]}
    ord_buf <+ pipe_in {|p| {p.dst => next_vc} }
    pipe_sent <= pipe_in     # Unreliable delivery for now
  end

  bloom :inbound_msg do
    recv_buf <= chn
    buf_chosen <= recv_buf {|m| m.ord_buf.at(ip_port, Bud::MapLattice).lt_eq(my_vc).when_true { m } }
    recv_buf <- buf_chosen

    pipe_out <= buf_chosen {|m| [m.dst, m.src, m.ident, m.payload]}
    ord_buf <+ buf_chosen {|m| m.ord_buf}
  end

  # bloom :msg_log do
  #   stdio <~ chn {|c| ["(#{@budtime}) Inbound message @ #{port}: #{[c.src, c.ident, c.payload].inspect}, msg VC = #{c.clock.inspect}, msg ord_buf = #{c.ord_buf.inspect}, local VC: #{my_vc.inspect}, local ord_buf: #{ord_buf.inspect}"]}
  #   stdio <~ pipe_out {|m| ["(#{@budtime}) Delivering message @ #{port}: #{m.ident}"]}
  # end
end
