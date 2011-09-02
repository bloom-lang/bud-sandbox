require 'rubygems'
require 'bud'

module MVKVSProtocol
  state do
    interface input, :kvput, [:client, :key, :version] => [:reqid, :value]
    interface input, :kvget, [:reqid] => [:key]
    interface output, :kvget_response, [:reqid, :key, :version] => [:value]
  end
end

module BasicMVKVS
  include MVKVSProtocol

  state do
    table :kvstate, [:key, :version] => [:value]
  end

  bloom :put do
    kvstate <+ kvput {|s|  [s.key, s.version, s.value]}
  end

  bloom :get do
    temp :getj <= (kvget * kvstate).pairs(:key => :key)
    kvget_response <= getj do |g, t|
      [g.reqid, t.key, t.version, t.value]
    end
  end
end
