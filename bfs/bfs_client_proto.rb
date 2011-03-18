require 'rubygems'
require 'bud'

module BFSClientProtocol
  state do
    interface input, :request, [:reqid] => [:rtype, :arg]
    interface output, :response, [:reqid] => [:status, :response]
  end
end

module BFSClientMasterProtocol
  state do
    channel :request_msg, [:@master, :source, :reqid, :rtype, :args]
    channel :response_msg, [:@source, :master, :reqid, :status, :response]
  end
end

module BFSHBProtocol
  state do
    channel :hb_ack
  end
end
