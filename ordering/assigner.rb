require 'rubygems'
require 'bud'
require 'backports'

require 'ordering/serializer'

module AssignerProto
  state do
    interface input, :dump, [:payload]
    interface output, :pickup, [:ident] => [:payload]
  end
end

module Assigner
  include AssignerProto
  include SerializerProto

  bloom :logos do
    enqueue <= dump do |d|
      [d.join(","), d]
    end

    dequeue <= localtick

    pickup <= dequeue_resp {|r| [r.ident, r.payload] }
  end
end

# This assigns IDs to facts inserted via "dump", and delivers the resulting [id,
# payload] pairs via "pickup". Note that the assignment of IDs does not require
# multiple timesteps, but it is also not deterministic: ID assignment is based
# on the internal order in which facts appear in collections, which is outside
# the semantics of Bloom.
module AggAssign
  include AssignerProto

  state do
    scratch :holder, [:array]
  end

  bloom :grouping do
    holder <= dump.group(nil, accum(dump.payload))
    #stdio <~ holder {|h| ["HOLD: #{h.inspect}"] }
    pickup <= holder.flat_map do |h|
      h.array.each_with_index.map {|a, i| [i, a] }
    end
  end
end

# This assigns IDs to facts inserted via "dump" in a single timestep. Unlike
# AggAssign, it achieves determinism by using the position of an element in a
# sorted sequence as the element's ID.
module SortAssign
  include AssignerProto

  bloom do
    pickup <= dump.sort.each_with_index.map {|a, i| [i, a]}
  end
end

# A common pattern: we generate IDs that are unique and increase over time by
# recording a persistent ID high-water mark, and updating it once per timestep.
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
    sub.dump <= dump
    pickup <= (sub.pickup * next_id).pairs do |p, n|
      [p.ident + n.val, p.payload]
    end
    dump_cnt <= dump.group(nil, count)
    next_id <+- (next_id * dump_cnt).pairs do |n, c|
      [n.val + c.cnt]
    end
  end
end
