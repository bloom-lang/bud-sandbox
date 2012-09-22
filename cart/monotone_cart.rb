require 'rubygems'
require 'bud'

require 'cart/cart_lattice'

module MonotoneCartProtocol
  state do
    channel :action_msg,
      [:@server, :session, :op_id] => [:item, :cnt]
    channel :checkout_msg,
      [:@server, :session, :op_id] => [:lbound, :addr]
    channel :response_msg,
      [:@client, :session] => [:items]
  end
end

module MonotoneReplica
  include MonotoneCartProtocol

  state { lmap :sessions }

  bloom do
    sessions <= action_msg do |m|
      c = CartLattice.new({m.op_id => [ACTION_OP, m.item, m.cnt]})
      { m.session => c }
    end

    sessions <= checkout_msg do |m|
      c = CartLattice.new({m.op_id => [CHECKOUT_OP, m.lbound, m.addr]})
      { m.session => c }
    end

    # XXX: Note that we will send an unbounded number of response messages for
    # each complete cart.
    response_msg <~ sessions.to_collection do |session, cart|
      cart.is_complete.when_true {
        [cart.checkout_addr, session, cart.summary]
      }
    end
  end
end

module MonotoneClient
  include MonotoneCartProtocol

  state do
    table :serv, [] => [:addr]
    scratch :do_action, [:session, :op_id] => [:item, :cnt]
    scratch :do_checkout, [:session, :op_id] => [:lbound]
  end

  bloom do
    action_msg <~ (do_action * serv).pairs do |a,s|
      [s.addr, a.session, a.op_id, a.item, a.cnt]
    end
    checkout_msg <~ (do_checkout * serv).pairs do |c,s|
      [s.addr, c.session, c.op_id, c.lbound, ip_port]
    end
  end
end
