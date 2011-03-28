require 'rubygems'
require 'bud'
require 'bfs/fs_master'
require 'bfs/hb_master'

module ChunkedFSProtocol
  include FSProtocol

  state do
    interface :input, :fschunklist, [:reqid, :file]
    interface :input, :fschunklocations, [:reqid, :chunkid]
    interface :input, :fsaddchunk, [:reqid, :file]
    # note that no output interface is defined.
    # we use :fsret (defined in FSProtocol) for output.
  end
end

module ChunkedKVSFS
  include ChunkedFSProtocol
  include KVSFS
  include HBMaster
  include TimestepNonce

  state do
    # master copy.  every chunk we ever tried to create metadata for.
    table :chunk, [:chunkid, :file, :siz]
    scratch :chunk_buffer, [:reqid, :chunkid]
    scratch :chunk_buffer2, [:reqid, :chunklist]
    scratch :host_buffer, [:reqid, :host]
    scratch :host_buffer2, [:reqid, :hostlist]
    scratch :lookup, [:reqid, :file]
  end

  bloom :lookups do
    lookup <= fschunklist
    lookup <= fsaddchunk

    kvget <= lookup { |a| [a.reqid, a.file] } 
    fsret <= lookup do |a|
      unless kvget_response.map{ |r| r.reqid}.include? a.reqid
        [a.reqid, false, "File not found: #{a.file}"]
      end
    end
  end

  bloom :getchunks do
    chunk_buffer <= (fschunklist * kvget_response * chunk).combos([fschunklist.reqid, kvget_response.reqid], [fschunklist.file, chunk.file]) { |l, r, c| [l.reqid, c.chunkid] }
    chunk_buffer2 <= chunk_buffer.group([chunk_buffer.reqid], accum(chunk_buffer.chunkid))
    fsret <= chunk_buffer2 { |c| [c.reqid, true, c.chunklist] }
    # handle case of empty file / haven't heard about chunks yet
  end

  bloom :getnodes do
    fsret <= fschunklocations do |l|
      unless chunk_cache.map{|c| c.chunkid}.include? l.chunkid
        [l.reqid, false, "no datanodes found for #{l.chunkid} in cc, now #{chunk_cache.length}"]
      end
    end

    # chunkjoin will have rows if the block above doesn't.
    temp :chunkjoin <= (fschunklocations * chunk_cache).pairs(:chunkid => :chunkid)
    host_buffer <= chunkjoin {|l, c| [l.reqid, c.node] }
    host_buffer2 <= host_buffer.group([host_buffer.reqid], accum(host_buffer.host))
    fsret <= host_buffer2 {|c| [c.reqid, true, c.hostlist] }
  end

  bloom :addchunks do
    #stdio <~ "Warning: no available datanodes" if available.empty?
    temp :minted_chunk <= (kvget_response * fsaddchunk * available * nonce).combos(kvget_response.reqid => fsaddchunk.reqid)
    chunk <= minted_chunk { |r, a, v, n| [n.ident, a.file, 0] }
    fsret <= minted_chunk { |r, a, v, n| [r.reqid, true, [n.ident, v.pref_list.slice(0, (REP_FACTOR + 2))]] }
    fsret <= (kvget_response * fsaddchunk).pairs(:reqid => :reqid) do |r, a|
      if available.empty? or available.first.pref_list.length < REP_FACTOR
        [r.reqid, false, "datanode set cannot satisfy REP_FACTOR = #{REP_FACTOR} with [#{available.first.nil? ? "NIL" : available.first.pref_list.inspect}]"]
      end
    end
  end
end
