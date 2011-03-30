require 'rubygems'
require 'bud'
require 'delivery/reliable_delivery'
require 'delivery/multicast'

module KVSProtocol
  state do
    interface input, :kvput, [:client, :key] => [:reqid, :value]
    interface input, :kvdel, [:key] => [:reqid]
    interface input, :kvget, [:reqid] => [:key]
    interface output, :kvget_response, [:reqid] => [:key, :value]
  end
end

module BasicKVS
  include KVSProtocol

  state do
    table :kvstate, [:key] => [:value]
  end

  bloom :mutate do
    kvstate <+ kvput {|s|  [s.key, s.value]}
    kvstate <- (kvstate * kvput).lefts(:key => :key)
  end

  bloom :get do
    temp :getj <= (kvget * kvstate).pairs(:key => :key)
    kvget_response <= getj do |g, t|
      [g.reqid, t.key, t.value]
    end
  end

  bloom :delete do
    kvstate <- (kvstate * kvdel).lefts(:key => :key)
  end
end


module ReplicatedKVS
  include KVSProtocol
  include MulticastProtocol
  import BasicKVS => :kvs

  bloom :local_indir do
    # if I am the master, multicast store requests
    send_mcast <= kvput do |k|
      unless member.include? [k.client]
        [k.reqid, [@addy, k.key, k.reqid, k.value]]
      end
    end

    kvs.kvput <= mcast_done {|m| m.payload }

    # if I am a replica, store the payload of the multicast
    kvs.kvput <= pipe_chan do |d|
      if d.payload.fetch(1) != @addy
        d.payload
      end
    end
  end
end

