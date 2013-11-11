require 'rubygems'
require 'bud'

require 'kvs/kvs'
require 'cart/cart_protocol'


module DestructiveCart
  include CartProtocol
  include KVSProtocol

  bloom :on_action do
    kvget <= action_msg {|a| [a.reqid, a.session] }
    kvput <= (action_msg * kvget_response).outer(:reqid => :reqid) do |a,r|
      val = r.value || {}
      [a.client, a.session, a.reqid, val.merge({a.item => a.cnt}) {|k,old,new| old + new}]
    end
  end

  bloom :on_checkout do
    kvget <= checkout_msg {|c| [c.reqid, c.session] }
    response_msg <~ (kvget_response * checkout_msg).pairs(:reqid => :reqid) do |r,c|
      [c.client, c.server, r.key, r.value.select {|k,v| v > 0}.sort]
    end
  end
end
