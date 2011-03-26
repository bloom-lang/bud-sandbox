require 'rubygems'
require 'bud'

require 'delivery/multicast'
require 'cart/cart_protocol'

module DisorderlyCart
  include CartProtocol

  state do
    table :cart_action, [:session, :reqid] => [:item, :action]
    scratch :action_cnt, [:session, :item, :action] => [:cnt]
    scratch :status, [:server, :client, :session, :item] => [:cnt]
  end

  bloom :saved do
    # store actions against the "cart;" that is, the session.
    cart_action <= action_msg.map { |c| [c.session, c.reqid, c.item, c.action] }
    action_cnt <= cart_action.group([cart_action.session, cart_action.item, cart_action.action], count(cart_action.reqid))
  end

  bloom :consider do
    status <= join([action_cnt, action_cnt, checkout_msg]).map do |a1, a2, c|
      if a1.session == a2.session and a1.item == a2.item and a1.session == c.session and a1.action == "Add" and a2.action == "Del"
        if (a1.cnt - a2.cnt) > 0
          puts "STAT" or [c.client, c.server, a1.session, a1.item, a1.cnt - a2.cnt]
        end
      end
    end
    status <= join([action_cnt, checkout_msg]).map do |a, c|
      if a.action == "Add" and not action_cnt.map{|d| d.item if d.action == "Del"}.include? a.item
        [c.client, c.server, a.session, a.item, a.cnt]
      end
    end

    temp :out <= status.reduce({}) do |memo, i|
      memo[[i[0],i[1],i[2]]] ||= []
      i[4].times do
        memo[[i[0],i[1],i[2]]] << i[3]
      end
      memo
    end.to_a

    response_msg <~ out.map do |k, v|
      k << v
    end
  end
end

module ReplicatedDisorderlyCart
  include DisorderlyCart
  include Multicast

  bloom :replicate do
    send_mcast <= action_msg.map {|a| [a.reqid, [a.session, a.reqid, a.item, a.action]]}
    cart_action <= mcast_done.map {|m| m.payload}
    cart_action <= pipe_chan.map {|c| c.payload}
  end
end
