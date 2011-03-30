require 'rubygems'
require 'bud'

require 'kvs/kvs'
require 'cart/cart_protocol'


module DestructiveCart
  include CartProtocol
  include KVSProtocol

  bloom :queueing do
    kvget <= action_msg {|a| puts "test" or [a.reqid, a.session] }
    kvput <= action_msg do |a| 
      if a.action == "Add" and not kvget_response.map{|b| b.key}.include? a.session
        puts "PUT EMPTY" or [a.client, a.session, a.reqid, Array.new.push(a.item)]
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
      puts "RESP" or [r.client, r.server, r.key, r.value, nil]
    end
  end
end
