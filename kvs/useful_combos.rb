require 'rubygems'
require 'bud'

require 'kvs/kvs'
require 'delivery/multicast'

# some combinations are simple:

# was TKV
class SingleSiteKVS
  include Bud
  include BasicKVS
end

# was RKV
class BestEffortReplicatedKVS
  include Bud
  include ReplicatedKVS
  include BestEffortMulticast
  include StaticMembership
end

class ReliableReplicatedKVS
  include Bud
  include ReplicatedKVS
  include ReliableMulticast
  include StaticMembership
end

module ReplicatedMeteredGlue
  # we have mixed in KVSMetering and ReplicatedKVS, both of which implement
  # indirection.  we need to compose these!
  
  # it's annoying to write this and unthinkable to imagine writing up all the
  # possible combinations of mostly orthogonal components.  I'd like to see this
  # as a rewrite, though different rewrites correspond to different strategies
  # for join order, materialization etc.
  state do
    table :cs_rep, ['ident'], ['payload']
    table :cs_meter, ['ident'], ['payload']
    scratch :rmg_can_store, ['ident'], ['payload']
  end

  bloom :indir do
    can_store <= rmg_can_store
  end

  bloom :rmg_indir do
    cs_rep <= rep_can_store
    cs_meter <= meter_can_store
    temp :csj <= (cs_rep * cs_meter).pairs(:ident => :ident)
    rmg_can_store <+ csj.map { |r, m| r } 
    cs_rep <- csj.map {|r, m| r }
    cs_meter <- csj.map {|r, m| m }
  end
end
