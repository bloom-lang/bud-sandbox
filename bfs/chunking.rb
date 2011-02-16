require 'rubygems'
require 'bud'
require 'bfs/fs_master'
require 'bfs/hb_master'

module ChunkedFSProtocol
  include FSProtocol

  state {
    interface :input, :fschunklocations, [:reqid, :file, :chunkid]
    interface :input, :fsnewchunk, [:reqid, :file]
  }
end

module ChunkedKVSFS
  include ChunkedFSProtocol
  include KVSFS
  include HBMaster

  state {
    scratch :chunk_buffer, [:reqid, :host]
    scratch :chunk_buffer2, [:reqid, :hostlist]
  }

  declare
  def getchunks
    fsret <= fschunklocations.map do |l|
      unless chunks.map{|c| [c.file, c.chunkid]}.include? [l.file, l.chunkid]
        puts "EMPTY for #{l.inspect}" or [l.reqid, false, nil]
      end
    end

    chunkjoin = join [fschunklocations, chunks], [fschunklocations.file, chunks.file], [fschunklocations.chunkid, chunks.chunkid]
    chunk_buffer <= chunkjoin.map{|l, c| puts "CHUNKBUFFER" or [l.reqid, c.node] }
    # what a hassle
    ##fsret <= chunk_buffer.group([chunk_buffer.reqid, true], accum(chunk_buffer.host))
    chunk_buffer2 <= chunk_buffer.group([chunk_buffer.reqid], accum(chunk_buffer.host))
    fsret <= chunk_buffer2.map{|c| [c.reqid, true, c.hostlist] }
  end
end
