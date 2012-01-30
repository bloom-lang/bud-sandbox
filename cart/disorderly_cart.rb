require 'rubygems'
require 'bud'

require 'delivery/multicast'
require 'cart/cart_protocol'

module DisorderlyCart
  include CartProtocol

  state do
    table :action_log, [:session, :reqid] => [:item, :action]
    scratch :item_sum, [:session, :item] => [:num]
    scratch :session_final, [:session] => [:items, :counts]
  end

  bloom :on_action do
    action_log <= action_msg { |c| [c.session, c.reqid, c.item, c.action] }
  end

  bloom :on_checkout do
    temp :checkout_log <= (checkout_msg * action_log).rights(:session => :session)
    item_sum <= checkout_log.group([action_log.session, action_log.item],
                                   sum(action_log.action)) do |s|
      # Don't return items with non-positive counts. XXX: "s" has no schema
      # information, so we can't reference the sum by column name.
      s if s.last > 0
    end
    session_final <= item_sum.group([:session], accum(:item), accum(:num))
    response_msg <~ (session_final * checkout_msg).pairs(:session => :session) do |c,m|
      [m.client, m.server, m.session, c.items.zip(c.counts).sort]
    end
  end
end

module ReplicatedDisorderlyCart
  include DisorderlyCart
  include Multicast

  bloom :replicate do
    mcast_send <= action_msg {|a| [a.reqid, [a.session, a.reqid, a.item, a.action]]}
    action_log <= mcast_done {|m| m.payload}
    action_log <= pipe_out {|c| c.payload}
  end
end
