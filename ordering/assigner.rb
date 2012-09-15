require 'rubygems'
require 'bud'

require 'ordering/serializer'

module AssignerProto
  state do
    interface input, :id_request, [:payload]
    interface output, :id_response, [:ident] => [:payload]
  end
end

#deprecated -- this code seems not to work.  Please use AggAssign below.
module Assigner
  include AssignerProto
  include SerializerProto

  bloom :logos do
    enqueue <= id_request do |d|
      [d.to_a.join(","), d]
    end

    dequeue <= localtick

    id_response <= dequeue_resp {|r| [r.ident, r.payload] }
  end
end

# This assigns IDs to facts inserted via "id_request", and delivers the resulting [id,
# payload] pairs via "id_response". Note that the assignment of IDs does not require
# multiple timesteps, but it is also not deterministic: ID assignment is based
# on the internal order in which facts appear in collections, which is outside
# the semantics of Bloom.
module AggAssign
  include AssignerProto

  state do
    scratch :holder, [:array]
  end

  bloom :grouping do
    holder <= id_request.group(nil, accum(id_request.payload))
    #stdio <~ holder {|h| ["HOLD: #{h.inspect}"] }
    id_response <= holder.flat_map do |h|
      h.array.each_with_index.map {|a, i| [i, a] }
    end
  end
end


# This assigns IDs to facts inserted via "id_request" in a single timestep. Unlike
# AggAssign, it achieves determinism by using the position of an element in a
# sorted sequence as the element's ID.
module SortAssign
  include AssignerProto

  bloom do
    id_response <= id_request.sort.each_with_index.map {|a, i| [i, a]}
  end
end

# A common pattern: we generate IDs that are unique and increase over time by
# recording a persistent ID high-water mark, and updating it once per timestep.
module AggAssignPersist
  include AssignerProto
  import AggAssign => :sub

  state do
    table :next_id, [] => [:val]
    scratch :dump_cnt, [] => [:cnt]
  end

  bootstrap do
    next_id <= [[0]]
  end

  bloom do
    sub.id_request <= id_request
    id_response <= (sub.id_response * next_id).pairs do |p, n|
      [p.ident + n.val, p.payload]
    end
    dump_cnt <= id_request.group(nil, count)
    next_id <+- (next_id * dump_cnt).pairs do |n, c|
      [n.val + c.cnt]
    end
  end
end

# XXXX-UGLINESS -- this is identical to AggAssignPersist, just imports a 
# different :sub!
module SortAssignPersist
  include AssignerProto
  import SortAssign => :sub

  state do
    table :next_id, [] => [:val]
    scratch :dump_cnt, [] => [:cnt]
  end

  bootstrap do
    next_id <= [[0]]
  end

  bloom do
    sub.id_request <= id_request
    id_response <= (sub.id_response * next_id).pairs do |p, n|
      [p.ident + n.val, p.payload]
    end
    dump_cnt <= id_request.group(nil, count)
    next_id <+- (next_id * dump_cnt).pairs do |n, c|
      [n.val + c.cnt]
    end
  end
end