require 'rubygems'
require 'bud'
require 'bfs/fs_master'
require 'bfs/hb_master'

module ChunkedFSProtocol
  include FSProtocol

  state {
    interface :input, :fschunklist, [:reqid, :file]
    interface :input, :fschunklocations, [:reqid, :chunkid]
    interface :input, :fsnewchunk, [:reqid, :file]
    interface :input, :fsaddchunk, [:reqid, :file]

    scratch :chunklist_buffer, [:reqid, :file, :chunkid]
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
    scratch :chunk_buffer, [:reqid, :chunkid]
    scratch :chunk_buffer2, [:reqid, :chunklist]
    scratch :host_buffer, [:reqid, :host]
    scratch :host_buffer2, [:reqid, :hostlist]
    scratch :lookup, [:reqid, :file]
  }

  declare 
  def lookups
    lookup <= fschunklist
    lookup <= fsaddchunk

    kvget <= lookup.map{ |a| puts "look up file: #{a.inspect}" or [a.reqid, a.file] } 
    fsret <= lookup.map do |a|
      unless kvget_response.map{ |r| r.reqid}.include? a.reqid
        puts "file lookup fail" or [a.reqid, false, "File not found: #{a.file}"]
      end
    end
  end

  declare
  def getchunks
    chunk_buffer <= join([fschunklist, kvget_response, chunk], [fschunklist.reqid, kvget_response.reqid], [fschunklist.file, chunk.file]).map{ |l, r, c| [l.reqid, c.chunkid] }
    chunk_buffer2 <= chunk_buffer.group([chunk_buffer.reqid], accum(chunk_buffer.chunkid))
    fsret <= chunk_buffer2.map{ |c| [c.reqid, true, c.chunklist] }

    # handle case of empty file / haven't heard about chunks yet

  end

  declare 
  def getnodes
    fsret <= fschunklocations.map do |l|
      unless chunk_cache.map{|c| c.chunkid}.include? l.chunkid
        puts "EMPTY for #{l.inspect}" or [l.reqid, false, nil]
      end
    end

    chunkjoin = join [fschunklocations, chunk_cache], [fschunklocations.chunkid, chunk_cache.chunkid]
    host_buffer <= chunkjoin.map{|l, c| puts "CHUNKBUFFER" or [l.reqid, c.node] }
    # what a hassle
    host_buffer2 <= host_buffer.group([host_buffer.reqid], accum(host_buffer.host))
    fsret <= host_buffer2.map{|c| [c.reqid, true, c.hostlist] }
  end

  declare
  def addchunks
    stdio <~ "Warning: no available datanodes" if available.empty?
    stdio <~ kvget_response.map{|r| ["#{budtime} kvg_r: #{r.inspect}, al #{available.length} (chunkcachec #{chunk_cache.length})" ] }
    
    minted_chunk = join([kvget_response, fsaddchunk, available, nonce], [kvget_response.reqid, fsaddchunk.reqid])
    chunk <= minted_chunk.map{ |r, a, v, n| [n.ident, a.file, 0] }
    fsret <= minted_chunk.map{ |r, a, v, n| puts "mINted chunk : @#{@budtime} #{n.ident} for #{a.file} with preflist #{v.pref_list.inspect}" or [r.reqid, true, [n.ident, v.pref_list]] }

    fsret <= join([kvget_response, fsaddchunk], [kvget_response.reqid, fsaddchunk.reqid]).map do |r, a|
      if available.empty?
        [r.reqid, false, "empty datanode set!"]
      end
    end
      
  end
end
