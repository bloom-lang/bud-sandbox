require 'rubygems'
require 'bud'
require 'bfs/data_protocol'
require 'bfs/bfs_config'
require 'bfs/hb_master'

BG_MULT = 1

# Background processes for the BFS master.  Right now, this is just firing off
# replication requests for chunks whose replication factor is too low.
module BFSBackgroundTasks
  state do
    interface output, :copy_chunk, [:chunkid, :owner, :newreplica]
    periodic :bg_timer, (MASTER_DUTY_CYCLE * BG_MULT)
    scratch :chunk_cnts_chunk, [:chunkid, :replicas]
    scratch :chunk_cnts_host, [:host, :chunks]
    scratch :candidate_nodes, [:chunkid, :host, :chunks]
    scratch :best_dest, candidate_nodes.schema
    scratch :chosen_dest, [:chunkid, :host]
    scratch :source, [:chunkid, :host]
    scratch :best_src, [:chunkid, :host]
    scratch :lowchunks, [:chunkid]
    scratch :cc_demand, chunk_cache_alive.schema
  end

  bloom :replication do
    cc_demand <= (bg_timer * chunk_cache_alive).rights
    cc_demand <= (bg_timer * last_heartbeat).pairs {|b, h| [h.peer, nil, nil]}
    chunk_cnts_chunk <= cc_demand.group([cc_demand.chunkid], count(cc_demand.node))
    chunk_cnts_host <= cc_demand.group([cc_demand.node], count(cc_demand.chunkid))

    lowchunks <= chunk_cnts_chunk { |c| [c.chunkid] if c.replicas < REP_FACTOR and !c.chunkid.nil?}

    # nodes in possession of such chunks
    source <= (cc_demand * lowchunks).pairs(:chunkid => :chunkid) {|a, b| [a.chunkid, a.node]}
    # nodes not in possession of such chunks, and their fill factor
    candidate_nodes <= (chunk_cnts_host * lowchunks).pairs do |c, p|
      unless chunk_cache_alive.map{|a| a.node if a.chunkid == p.chunkid}.include? c.host
        [p.chunkid, c.host, c.chunks]
      end
    end

    best_dest <= candidate_nodes.argagg(:min, [candidate_nodes.chunkid], candidate_nodes.chunks)
    chosen_dest <= best_dest.group([best_dest.chunkid], choose(best_dest.host))
    best_src <= source.group([source.chunkid], choose(source.host))
    copy_chunk <= (chosen_dest * best_src).pairs(:chunkid => :chunkid) do |d, s|
      [d.chunkid, s.host, d.host]
    end
  end
end
