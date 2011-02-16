require 'rubygems'
require 'bud'

module BFSClientProtocol
  include BudModule

  state {
    interface input, :request, ['reqid', 'type', 'arg']
    interface output, :response, ['reqid', 'response']

    channel :request_msg, ['@master', 'source', 'reqid', 'type', 'args']
  }
end


module BFSClient
  include BFSClientProtocol

  def initialize(i, p, o)
    @addy = "#{i}:#{p}"
    super
  end

  state {
    table :master, [], ['master']
  }

  declare
  def cglue
    request_msg <~ join([request, master]).map{|r, m| [m.master, @addy, r.reqid, r.type, r.args] }
  end
end
