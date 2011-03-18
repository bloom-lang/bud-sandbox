require 'rubygems'
require 'bud'
require 'bfs/bfs_client_proto'
require 'bfs/background'
require 'bfs/chunking'

# glues together an implementation of a ChunkedFS with the BFSClientProtocol protocol

module BFSMasterGlue
  include BFSClientMasterProtocol
    
  state do
    table :rendez, request_msg.schema
  end

  bloom :mglue do
    stdio <~ request_msg.map{ |r| ["request: #{r.inspect}"] } 

    rendez <= request_msg
    fscreate <= request_msg.map{ |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "create" }
    fsmkdir <= request_msg.map{ |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "mkdir" }
    fsls <= request_msg.map{ |r| [r.reqid, r.args] if r.rtype == "ls" } 
    fsaddchunk <= request_msg.map{ |r| [r.reqid, r.args] if r.rtype == "append" } 
    fschunklist <= request_msg.map{ |r| [r.reqid, r.args] if r.rtype == "getchunks" }       
    fschunklocations <= request_msg.map{ |r| [r.reqid, r.args] if r.rtype == "getchunklocations" }       
    fsrm <= request_msg.map{ |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "rm" }
  end

  bloom :response_glue do
    response_msg <~ join([fsret, rendez], [fsret.reqid, rendez.reqid]).map do |f, r|
      [r.source, r.master, f.reqid, f.status, f.data]
    end
  end 
end


class BFSMasterServer
  # the completely composed BFS
  include Bud
  include ChunkedKVSFS
  include HBMaster
  include BFSMasterGlue
  include BFSBackgroundTasks
  include StaticMembership

  # and its background tasks
  def run_bg
    # EventMachine must be running before we can start a listener
    super
    start_background_task_thread
  end

  def start_background_task_thread
    meeting = Rendezvous.new(self, self.copy_chunk)
    Thread.new do 
      loop do
        task = meeting.block_on(1000)
        bg_besteffort_request(task[0], task[1], task[2])
      end
    end
  end

  def bg_besteffort_request(c, o, r)
    DataProtocolClient.send_replicate(c, r, o)
  end
end

