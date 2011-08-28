require 'rubygems'
require 'bud'
require 'delivery/reliable_delivery'
require 'delivery/multicast'
require 'lckmgr/lckmgr'
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

module PersistentKVS
  include KVSProtocol
  #import BasicKVS => :kvs
  include BasicKVS

  state do
    sync :kvstate_backing, :bud, kvstate.schema
  end

  bootstrap do
    puts "BOOTZ:"
    kvstate <= kvstate_backing {|b| puts "BACK: #{b.inspect}"; b}
  end

  bloom do
    #stdio <~ kvstate_backing.inspected
    kvstate <+ kvstate_backing do |b| 
      if kvstate.empty?
        puts "EMPTY"
        b
    #  else
    #    puts "not empty"
      end
    end
    # declaratively ok. 
    kvstate_backing <= kvstate
  end
end


module ReplicatedKVS
  include KVSProtocol
  include MulticastProtocol
  import BasicKVS => :kvs

  bloom :local_indir do
    kvget_response <= kvs.kvget_response
    kvs.kvdel <= kvdel
    kvs.kvget <= kvget

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
