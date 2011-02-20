require 'rubygems'
require 'bud'
require 'bfs/bfs_client_proto'

module BFSMasterServer
  include BFSClientProtocol
    
  state {
    table :rendez, request_msg.schema
  }

  declare
  def mglue
    rendez <= request_msg
    stdio <~ request_msg.map{ |r| ["request: #{r.inspect}"] } 
    fscreate <= request_msg.map{ |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "create" }
    fsmkdir <= request_msg.map{ |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "mkdir" }
    fsls <= request_msg.map{ |r| puts "ls request" or [r.reqid, r.args] if r.rtype == "ls" } 
    fsaddchunk <= request_msg.map{ |r| puts "append request" or [r.reqid, r.args] if r.rtype == "append" } 
      
  end

  declare
  def response_glue
    response_msg <~ join([fsret, rendez], [fsret.reqid, rendez.reqid]).map do |f, r|
      [r.source, r.master, f.reqid, f.status, f.data]
    end
  end 
end
