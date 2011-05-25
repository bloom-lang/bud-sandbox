require 'rubygems'
require 'bud'

module CartWorkloads
  def run_cart(program, client)
    addy = "#{program.ip}:#{program.port}"
    contact = client.nil? ? program : client
    contact.async_do {
      contact.client_action <+ [[addy, 1234, 123, 'meat', 'Add']]
      contact.client_action <+ [[addy, 1234, 124, 'beer', 'Add']]
      contact.client_action <+ [[addy, 1234, 125, 'diapers', 'Add']]
      contact.client_action <+ [[addy, 1234, 126, 'meat', 'Del']]


      contact.client_action <+ [[addy, 1234, 127, 'beer', 'Add']]
      contact.client_action <+ [[addy, 1234, 128, 'beer', 'Add']]
      contact.client_action <+ [[addy, 1234, 129, 'beer', 'Add']]
      contact.client_action <+ [[addy, 1234, 130, 'beer', 'Del']]
    }
    contact.sync_do{}
    # block until we see the checkout message come in
    contact.sync_callback(:client_checkout, [[addy, 1234, 131]], :response_msg)
  end

  def run_cart2(program)
    addy = "#{program.ip}:#{program.port}"
    add_members(program, addy)
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'meat', 'Add', 123])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'Add', 124])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'diapers', 'Add', 125])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'meat', 'Del', 126])

    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'Add', 127])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'Add', 128])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'Add', 129])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'Del', 130])


    send_channel(program.ip, program.port, "checkout_msg", [addy, addy,1234, 131])
    advance(program)
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'papers', 'Add', 132])
    advance(program)    
  end
end
