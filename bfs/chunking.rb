require 'rubygems'
require 'bud'
require 'bfs/fs_master'
require 'bfs/hb_master'

module ChunkedFSProtocol
  include FSProtocol

  state {
    interface :input, :fschunklocations, [:reqid, :file, :chunkid]
    interface :input, :fsnewchunk, [:reqid, :file]
    interface :input, :fsaddchunk, [:reqid, :file]
  }
end

module ChunkedKVSFS
  include ChunkedFSProtocol
  include KVSFS
  include HBMaster
  include SimpleNonce

  state {
    # master copy.  every chunk we ever tried to create metadata for.
    table :chunk, [:chunkid, :file, :siz]
    scratch :chunk_buffer, [:reqid, :host]
    scratch :chunk_buffer2, [:reqid, :hostlist]
  }

  declare
  def getchunks
    fsret <= fschunklocations.map do |l|
      unless chunk_cache.map{|c| [c.file, c.chunkid]}.include? [l.file, l.chunkid]
        puts "EMPTY for #{l.inspect}" or [l.reqid, false, nil]
      end
    end

    chunkjoin = join [fschunklocations, chunk_cache], [fschunklocations.file, chunk_cache.file], [fschunklocations.chunkid, chunk_cache.chunkid]
    chunk_buffer <= chunkjoin.map{|l, c| puts "CHUNKBUFFER" or [l.reqid, c.node] }
    # what a hassle
    ##fsret <= chunk_buffer.group([chunk_buffer.reqid, true], accum(chunk_buffer.host))
    chunk_buffer2 <= chunk_buffer.group([chunk_buffer.reqid], accum(chunk_buffer.host))
    fsret <= chunk_buffer2.map{|c| [c.reqid, true, c.hostlist] }
  end

  declare
  def addchunks
    kvget <= fsaddchunk.map{ |a| puts "look up chunk: #{a.inspect}" or [a.reqid, a.file] } 
    fsret <= fsaddchunk.map do |a|
      unless kvget_response.map{ |r| r.reqid}.include? a.reqid
        puts "chunk lookup fail" or [a.reqid, false, nil]
      end
    end

    stdio <~ "Warning: no available datanodes" if available.empty?
    stdio <~ kvget_response.map{|r| ["kvg_r: #{r.inspect}, al #{available.length}"] }
    
    minted_chunk = join([kvget_response, fsaddchunk, available, nonce], [kvget_response.reqid, fsaddchunk.reqid])
    chunk <= minted_chunk.map{ |r, a, v, n| [n.ident, a.file, 0] }
    fsret <= minted_chunk.map{ |r, a, v, n| puts "mINted chunk : @#{@budtime} #{n.ident} for #{a.file}" or [r.reqid, true, [n.ident, v.pref_list]] }
      
  end
end
