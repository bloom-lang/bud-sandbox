require 'rubygems'
require 'bud'

module CartWorkloads
  def run_cart(program, client, actions=12)
    workload = [['meat', 1],
                ['books', -1],
                ['beer', 1],
                ['diapers', 1],
                ['meat', -1]]
    actions.times do |i|
      workload << ['beer', 1]
    end

    addy = program.ip_port
    client.sync_do {
      workload.each_with_index do |w, i|
        client.client_action <+ [[addy, 1234, gen_seq] + w]
      end
    }

    # block until we see the checkout message come in
    client.sync_callback(:client_checkout, [[addy, 1234, gen_seq]],
                         :response_msg)
  end

  def gen_seq
    @seq ||= 0
    @seq += 1
  end
end
