require 'rubygems'
require 'bud'

module CartProtocol
  state do
    # PAA -- took the '@'s off all occurrences of :server below
    channel :action_msg,
      [:@server, :client, :session, :reqid] => [:item, :cnt]
    channel :checkout_msg,
      [:@server, :client, :session, :reqid]
    # Upon receiving a checkout_msg, the server responds with a single
    # response_msg; the nested "items" array contains pairs of [item_id, count].
    channel :response_msg,
      [:@client, :server, :session] => [:items]
  end
end

module CartClientProtocol
  state do
    interface input, :client_checkout, [:server, :session, :reqid]
    interface input, :client_action, [:server, :session, :reqid] => [:item, :cnt]
    # XXX: why does this have "client" as a field?
    interface output, :client_response, [:client, :server, :session] => [:items]
  end
end

module CartClient
  include CartProtocol
  include CartClientProtocol

  bloom :client do
    action_msg <~ client_action {|a| [a.server, ip_port, a.session, a.reqid, a.item, a.cnt]}
    checkout_msg <~ client_checkout {|a| [a.server, ip_port, a.session, a.reqid]}
    client_response <= response_msg
  end
end
