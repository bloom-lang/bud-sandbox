require 'rubygems'
require 'bud'

module SerializerProto
  include BudModule

  state {
    interface input, :enqueue, [:ident] =>  [:payload]
    interface input, :dequeue, [] => [:reqid]
    interface output, :dequeue_resp, [:reqid] => [:ident, :payload]
  }
end

module Serializer
  include SerializerProto

  state {
    table :storage_tab, [:ident, :payload]
    scratch :top, [:ident]
  }

  bootstrap do
    #localtick <~ [[@budtime]]
  end

  declare 
  def logic
    storage_tab <= enqueue
    top <= storage_tab.group(nil, min(storage_tab.ident))
  end
  
  #declare
  #def clock
  #  localtick <~ enqueue.map{|s| [s] }
  #  localtick <~ dequeue.map{|s| [s] }
  #end

  declare
  def actions
    deq = join [storage_tab, top, dequeue], [storage_tab.ident, top.ident]
    dequeue_resp <+ deq.map do |s, t, d|
      [d.reqid, s.ident, s.payload]
    end
    storage_tab <- deq.map {|s, t, d| s }
    #localtick <= deq.map{|s, t, d| d }
  end
end
