require 'rubygems'
require 'bud'
require 'bfs/data_protocol'

REP_FACTOR = 2


# Background processes for the BFS master.  Right now, this is just firing off replication requests 
# for chunks whose replication factor is too low.


module BFSBackgroundTasks
  include BudModule

  state do
    interface output, :copy_chunk, [:chunkid, :owner, :newreplica]
    periodic :bg_timer, 1
    scratch :chunk_cnts_chunk, [:chunkid, :replicas]
    scratch :chunk_cnts_host, [:host, :chunks]
    scratch :candidate_nodes, [:chunkid, :host, :chunks]
    scratch :best_dest, [:chunkid, :host]

    scratch :sources, [:chunkid, :host]
    scratch :best_src, [:chunkid, :host]
  end

  declare
  def replication
    chunk_cnts_chunk <= chunk_cache.group([chunk_cache.chunkid], count(chunk_cache.node))
    chunk_cnts_host <= chunk_cache.group([chunk_cache.node], count(chunk_cache.chunkid))

    stdio <~ join([bg_timer, chunk_cnts_chunk, chunk_cache, chunk_cnts_host], [chunk_cnts_chunk.chunkid, chunk_cache.chunkid], [chunk_cache.node, chunk_cnts_host.host]).map do |t, c, cc, h| 
      #["CCC: #{c.inspect}, CC: #{cc.inspect}, H: #{h.inspect}"]
    end


    danger = join [bg_timer, chunk_cnts_chunk, chunk_cache, chunk_cnts_host], [chunk_cache.node, chunk_cnts_host.host]
    # crazy to assemble all this...
    candidate_nodes <= danger.map do |t, ccc, cc, cch|
      if ccc.replicas < REP_FACTOR
        #puts "REP FAC LOW"
        unless chunk_cache.map{|c| c.node if c.chunkid == ccc.chunkid}.include?  cc.node
          [ccc.chunkid, cc.node, cch.chunks]
        end
      end
    end


    best_dest <= candidate_nodes.argagg(:min, [candidate_nodes.chunkid, candidate_nodes.host], candidate_nodes.chunks)


    
    sources <= join([chunk_cache, candidate_nodes], [chunk_cache.chunkid, candidate_nodes.chunkid]).map{|c, cn| [c.chunkid, c.node]}
    best_src <= sources.group([sources.chunkid], choose(sources.host))
    copy_chunk <= join([best_dest, best_src], [best_dest.chunkid, best_src.chunkid]).map do |d, s|
      [d.chunkid, s.host, d.host]
    end

    stdio <~ copy_chunk.map{|c| bg_besteffort_request(c.chunkid, c.owner, c.newreplica) or ["COPY CHUNK: #{c.inspect}"] } 

  end
  
  def bg_besteffort_request(c, o, r)
    # todo: event machine schedule
    DataProtocolClient.send_replicate(c, r, o)    
  end
end
