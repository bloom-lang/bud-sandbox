require 'rubygems'
require 'bud'
require 'ordering/vector_clock'

module MVKVSProtocol
  state do
    interface input, :kvput, [:client, :key, :version] => [:reqid, :value]
    interface input, :kvget, [:reqid] => [:key]
    interface output, :kvget_response, [:reqid, :key, :version] => [:value]
  end
end

module MVKVS_state
  state do
    table :kvstate, [:key, :version] => [:value]
  end
end

module MVKVS_get
  include MVKVSProtocol

  bloom :get do
    temp :getj <= (kvget * kvstate).pairs(:key => :key)
    kvget_response <= getj do |g, t|
      [g.reqid, t.key, t.version, t.value]
    end
  end
end

module BasicMVKVS
  include MVKVSProtocol
  include MVKVS_get
  include MVKVS_state
  
  bloom :put do
    kvstate <+ kvput {|s|  [s.key, s.version, s.value]}
  end
end

#auto-increments vector clock on insert
#returns all matching vectors in DB on return
#vector merging, etc. needs to be handled by client/frontend module
module VC_MVKVS
  include MVKVSProtocol
  include MVKVS_get
  include MVKVS_state
  
  bloom :put do
    kvstate <+ kvput do |s|
      s.version.increment(s.client)
      [s.key, s.version.clone, s.value]
    end
  end
end
