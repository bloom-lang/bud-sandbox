require 'rubygems'
require 'bud'

module SerializerProto
  state do
    interface input, :enqueue, [:ident] =>  [:payload]
    interface input, :dequeue, [] => [:reqid]
    interface output, :dequeue_resp, [:reqid] => [:ident, :payload]
  end
end

module Serializer
  include SerializerProto

  state do
    table :storage_tab, [:ident, :payload]
    scratch :top, [:ident]
  end

  # bootstrap do
  #   localtick <~ [[@budtime]]
  # end

  bloom :logic do
    storage_tab <= enqueue
    top <= storage_tab.group(nil, min(storage_tab.ident))
  end
  
  #bloom :clock do
  #  localtick <~ enqueue {|s| [s] }
  #  localtick <~ dequeue {|s| [s] }
  #end

  bloom :actions do
    temp :deq <= (storage_tab * top * dequeue).combos(storage_tab.ident => top.ident)
    dequeue_resp <+ deq do |s, t, d|
      [d.reqid, s.ident, s.payload]
    end
    storage_tab <- deq {|s, t, d| s }
    #localtick <= deq {|s, t, d| d }
  end
end
