require 'rubygems'
require 'bud'
require 'heartbeat/heartbeat'

module BFSDatanode
  include Anise
  include HeartbeatAgent
  include StaticMembership
  annotator :declare

  def state
    super
    table :chunks, [:file, :ident, :size]
    scratch :chunk_summary, [:payload]
  end

  def bootstrap
    # fake; we'd read these from the fs
    # in the original bfs, we actually polled a directory, b/c
    # the chunks were written by an external process.
    chunks <+ [[1, 1, 1], [1, 2, 1], [1, 3, 1]]
  end

  declare 
  def hblogic
    chunk_summary <= chunks.map{|c| [[c.file, c.ident]] } 
    payload <= chunk_summary.group(nil, accum(chunk_summary.payload))
  end

  
end
