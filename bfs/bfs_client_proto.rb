require 'rubygems'
require 'bud'

module BFSClientProtocol
  include BudModule
  state do
    interface input, :request, [:reqid] => [:rtype, :arg]
    interface output, :response, [:reqid] => [:status, :response]
  end
end

module BFSClientMasterProtocol
  include BudModule
  state do
    channel :request_msg, [:@master, :source, :reqid, :rtype, :args]
    channel :response_msg, [:@source, :master, :reqid, :status, :response]
  end
end
