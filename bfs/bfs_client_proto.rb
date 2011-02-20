require 'rubygems'
require 'bud'

module BFSClientProtocol
  include BudModule

  state {
    interface input, :request, [:reqid] => [:rtype, :arg]
    interface output, :response, [:reqid] => [:status, :response]

    channel :request_msg, [:@master, :source, :reqid, :rtype, :args]
    channel :response_msg, [:@source, :master, :reqid, :status, :response]
  }
end
