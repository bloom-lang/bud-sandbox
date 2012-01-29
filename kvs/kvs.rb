require 'rubygems'
require 'bud'
require 'delivery/reliable'
require 'delivery/multicast'
require 'ordering/nonce'
require 'ordering/serializer'

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
    kvstate <+ kvput {|s| [s.key, s.value]}
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

module PersistentKVS
  include KVSProtocol
  include BasicKVS

  state do
    sync :kvstate_backing, :dbm, kvstate.schema
  end

  bootstrap do
    kvstate <= kvstate_backing
  end

  bloom do
    kvstate <+ kvstate_backing do |b|
      if kvstate.empty?
        b
      end
    end
    kvstate_backing <+ kvstate
    kvstate_backing <- kvstate_backing.notin(kvstate, :key => :key)
    kvstate_backing <- (kvstate_backing * kvstate).pairs(:key => :key) do |b, s|
      if b.value != s.value
        b
      end
    end
  end
end

module ReplicatedKVS
  include KVSProtocol
  include MulticastProtocol
  import BasicKVS => :kvs
  #import LSKVS => :kvs

  bloom :local_indir do
    kvget_response <= kvs.kvget_response
    kvs.kvdel <= kvdel
    kvs.kvget <= kvget

  end

  bloom :puts do

    # if I am the master, multicast store requests
    mcast_send <= kvput do |k|
      unless member.include? [k.client]
        [k.reqid, [:put, [@addy, k.key, k.reqid, k.value]]]
      end
    end

    kvs.kvput <= mcast_done do |m|
      if m.payload[0] == :put
        m.payload[1]
      end
    end

    # if I am a replica, store the payload of the multicast
    kvs.kvput <= pipe_out do |d|
      if d.payload.fetch(1) != @addy and d.payload[0] == "put"
        d.payload[1]
      end
   end
  end

  bloom :dels do
    mcast_send <= kvdel do |k|
      unless member.include? [k.client]
        [k.reqid, [:del, [@addy, k.key, k.reqid]]]
      end
    end

    kvs.kvdel <= mcast_done do |m|
      if m.payload[0] == :del
        m.payload[1]
      end
    end

    kvs.kvdel <= pipe_out do |d|
      if d.payload.fetch(1) != @addy and d.payload[0] == "del"
        d.payload[1]
      end
    end
  end
end
