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
    scratch :chunk_buffer, [:reqid, :host]
    scratch :chunk_buffer2, [:reqid, :hostlist]
  }

  declare
  def getchunks
    kvget <= fschunklist.map{ |l| [l.reqid, l.file] }
    fsret <= fschunklist.map do |l|
      unless kvget_response.map{ |r| r.reqid}.include? l.reqid
        [l.reqid, false, "file #{l.file} not found"]
      end
    end 

    
    #j_lookup = join [fschunklist, kvget_response], [fschunklist.reqid, kvget_response.reqid]
    #j_get_chunk = leftjoin [j_lookup, chunk], [j_lookup.file, chunk.file]

    #fsret <= j_get_chunk.map do |l, r, c|
    #  [l.reqid, false, "file #{file} is empty"] if c.nil?
    #end



    #fsret <= j_get_chunk.map do |l, r, c|
    #  [l.reqid, true, 
    #end
    
    # of course, it is possible that a file exists which is empty (or for which we haven't yet
    # received any chunk notifications).  Read empty file = fail.
    #j_get_chunk = join [fschunklist, kvget_response], [fschunklist.reqid, kvget_response.reqid]
    #fsret <= j_get_chunk.map do |r, k|
    #  unless chunk.map{ |c| c.file}.include? r.file
    #    [r.reqid, false, "file #{r.file} has no data"]
    #  end
    #end

    #j_get_chunk2 = join [j_get_chunk, chunk], [j_get_chunk.file, chunk.file]
    #fsret <= j_get_chunk2.group do |g, c|
    #  [
    #end
  end

  declare 
  def getnodes
    fsret <= fschunklocations.map do |l|
      unless chunk_cache.map{|c| c.chunkid}.include? l.chunkid
        puts "EMPTY for #{l.inspect}" or [l.reqid, false, nil]
      end
    end

    chunkjoin = join [fschunklocations, chunk_cache], [fschunklocations.chunkid, chunk_cache.chunkid]
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
