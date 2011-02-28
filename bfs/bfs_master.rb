require 'rubygems'
require 'bud'
require 'bfs/bfs_client_proto'

# glues together an implementation of a ChunkedFS with the BFSClientProtocol protocol

module BFSMasterServer
  include BFSClientProtocol
    
  state {
    table :rendez, request_msg.schema
  }

  declare
  def mglue
    #stdio <~ request_msg.map{ |r| ["request: #{r.inspect}"] } 

    rendez <= request_msg
    fscreate <= request_msg.map{ |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "create" }
    fsmkdir <= request_msg.map{ |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "mkdir" }
    fsls <= request_msg.map{ |r| [r.reqid, r.args] if r.rtype == "ls" } 
    fsaddchunk <= request_msg.map{ |r| [r.reqid, r.args] if r.rtype == "append" } 
    fschunklist <= request_msg.map{ |r| [r.reqid, r.args] if r.rtype == "getchunks" }       
    fschunklocations <= request_msg.map{ |r| [r.reqid, r.args] if r.rtype == "getchunklocations" }       
    fsrm <= request_msg.map{ |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "rm" }

  end

  declare
  def response_glue
    response_msg <~ join([fsret, rendez], [fsret.reqid, rendez.reqid]).map do |f, r|
      [r.source, r.master, f.reqid, f.status, f.data]
    end
  end 
end
