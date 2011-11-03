require 'rubygems'
require 'bud'

require 'kvs/kvs'
require 'cart/cart_protocol'


module DestructiveCart
  include CartProtocol
  include KVSProtocol
  
  def delete_one(a, i)
    a.delete_at(a.index(i)) unless a.index(i).nil?
    return a
  end

  bloom :queueing do
    kvget <= action_msg {|a| [a.reqid, a.session] }
    kvput <= action_msg do |a| 
      if a.action == "Add" and not kvget_response.map{|b| b.key}.include? a.session
        [a.client, a.session, a.reqid, Array.new.push(a.item)]
      end
    end

    temp :old_state <= (kvget_response * action_msg).pairs(:key => :session)
    kvput <= old_state do |b, a| 
      if a.action == "Add"
        [a.client, a.session, a.reqid, (b.value.clone.push(a.item))]
      elsif a.action == "Del"
        [a.client, a.session, a.reqid, delete_one(b.value, a.item)]
      end
    end
  end

  bloom :finish do
    kvget <= checkout_msg{|c| [c.reqid, c.session] }
    temp :lookup <= (kvget_response * checkout_msg).pairs(:key => :session)
    response_msg <~ lookup do |r, c|
      [c.client, c.server, r.key, r.value, nil]
    end
  end
end
