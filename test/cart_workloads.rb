require 'rubygems'
require 'bud'

module CartWorkloads
  def run_cart(program, client, actions=3)
    addy = "#{program.ip}:#{program.port}"
    contact = client.nil? ? program : client
    contact.async_do {
      contact.client_action <+ [[addy, 1234, 123, 'meat', 'Add']]
      contact.client_action <+ [[addy, 1234, 124, 'beer', 'Add']]
      contact.client_action <+ [[addy, 1234, 125, 'diapers', 'Add']]
      contact.client_action <+ [[addy, 1234, 126, 'meat', 'Del']]

      (0..actions).each do |i|
        contact.client_action <+ [[addy, 1234, 127 + i, 'beer', 'Add']]
      end
    }
    # XXX: why do we need 3 waits for each Bud instance?
    contact.sync_do
    contact.sync_do
    contact.sync_do
    program.sync_do
    program.sync_do
    program.sync_do
    
    # block until we see the checkout message come in
    contact.sync_callback(:client_checkout, [[addy, 1234, 131]], :response_msg)
  end
end
