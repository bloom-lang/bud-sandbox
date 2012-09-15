require 'rubygems'
require 'bud'

require 'cart/cart_lattice'

module MonotoneCartProtocol
  state do
    channel :action_msg,
      [:@server, :client, :session, :reqid] => [:item, :action]
    channel :checkout_msg,
      [:@server, :client, :session, :reqid] => [:lbound]
    channel :response_msg,
      [:@client, :server, :session] => [:items]
  end
end

module MonotoneReplica
  include MonotoneCartProtocol

  state do
    lmap :sessions
  end

  bloom :on_action do
    sessions <= action_msg {|c| { c.session => CartLattice.new({c.reqid => [ACTION_OP, c.item, c.action]}) } }
  end

  bloom :on_checkout do
    sessions <= checkout_msg {|c| { c.session => CartLattice.new({c.reqid => [CHECKOUT_OP, c.lbound, c.client]}) } }

    # XXX: Note that we will send an unbounded number of response messages for
    # each complete cart.
    response_msg <~ sessions.to_collection do |s_id, c|
      c.is_complete.when_true { [c.checkout_addr, ip_port, s_id, c.summary] }
    end
  end
end

module MonotoneClient
  include MonotoneCartProtocol

  state do
    table :serv, [] => [:addr]
    scratch :do_action, [:session, :reqid] => [:item, :action]
    scratch :do_checkout, [:session, :reqid] => [:lbound]
  end

  bloom do
    action_msg <~ (do_action * serv).pairs do |a,s|
      [s.addr, ip_port, a.session, a.reqid, a.item, a.action]
    end
    checkout_msg <~ (do_checkout * serv).pairs do |c,s|
      [s.addr, ip_port, c.session, c.reqid, c.lbound]
    end
    stdio <~ response_msg {|m| ["Response: #{m.inspect}"] }
  end
end

class ReplicaProgram
  include Bud
  include MonotoneReplica
end

class ClientProgram
  include Bud
  include MonotoneClient
end

# Simple test case
s = ReplicaProgram.new
s.run_bg
c = ClientProgram.new
c.run_bg

# Session 10: req IDs 5,6,7,8
# Session 11: req IDs 4,5,6
c.sync_do {
  c.serv <+ [[s.ip_port]]
  c.do_action <+ [[11, 5, 'beer', 2]]
  c.do_action <+ [[10, 7, 'vodka', -1]]
  c.do_checkout <+ [[10, 8, 5]]
}

c.sync_do {
  c.do_action <+ [[10, 5, 'vodka', 2], [10, 6, 'coffee', 7]]
  c.do_checkout <+ [[11, 6, 5]]
}

c.delta(:response_msg)
sleep 2

s.stop
c.stop
