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


module AggAssign
  include AssignerProto

  state do
    scratch :holder, [:array]
    scratch :holder2, [:array]
  end

  bloom :grouping do
    holder <= dump.group(nil, accum(dump.payload))
    #stdio <~ holder {|h| ["HOLD: #{h.inspect}"] }
    pickup <= holder.flat_map do |h|
      h.array.each_with_index.map{|a, i| [i, a] }
    end
  end
end
