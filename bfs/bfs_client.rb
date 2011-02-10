require 'rubygems'
require 'bud'

module BFSClientProtocol
  def state
    interface input, :request, ['reqid', 'type', 'arg']
    interface output, :response, ['reqid', 'response']

    channel :request_msg, ['@master', 'source', 'reqid', 'type', 'args']
  end
end



module BFSClient
  include Anise
  include BFSClientProtocol
  annotator :declare

  def initialize(i, p, o)
    @addy = "#{i}:#{p}"
    super
  end

  def state
    super
    table :master, [], ['master']
  end

  declare 
  def cglue 
    request_msg <~ join([request, master]).map{|r, m| [m.master, @addy, r.reqid, r.type, r.args] } 
  end
end
