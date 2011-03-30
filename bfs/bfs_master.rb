require 'rubygems'
require 'bud'
require 'bud/rendezvous'
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
    rendez <= request_msg
    fscreate <= request_msg { |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "create" }
    fsmkdir <= request_msg { |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "mkdir" }
    fsls <= request_msg { |r| [r.reqid, r.args] if r.rtype == "ls" } 
    fsaddchunk <= request_msg { |r| [r.reqid, r.args] if r.rtype == "append" } 
    fschunklist <= request_msg { |r| [r.reqid, r.args] if r.rtype == "getchunks" }       
    fschunklocations <= request_msg { |r| [r.reqid, r.args] if r.rtype == "getchunklocations" }       
    fsrm <= request_msg { |r| [r.reqid, r.args[0], r.args[1]] if r.rtype == "rm" }
  end

  bloom :response_glue do
    stdio <~ fsret.map{|r| ["return: #{r.inspect}"] }
    response_msg <~ (fsret * rendez).pairs(:reqid => :reqid) do |f, r|
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
    @task = register_callback(:copy_chunk) do |cb|
      cb.each do |t|
        bg_besteffort_request(t[0], t[1], t[2])
      end
    end
  end

  def stop_bg
    unregister_callback(@task)
    super
  end

  def bg_besteffort_request(c, o, r)
    DataProtocolClient.send_replicate(c, r, o)
  end
end

