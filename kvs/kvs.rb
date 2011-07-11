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

module TwoPLTransactionalKVS
  include KVSProtocol
  #import BasicKVS => :kvs
  include  BasicKVS
  include TwoPhaseLockMgr
  include TimestepNonce

  state do
    interface input, :xput, [:xid, :key, :data]
    interface input, :xget, [:xid, :key]
    interface output, :xget_response, [:xid, :key] => [:data]
    interface output, :xput_response, [:xid, :key]

    table :xput_buf, xput.schema
    table :xget_buf, xget.schema

    scratch :goods, xget.schema
  end

  bloom do
    xput_buf <= xput
    xget_buf <= xget
    request_lock <= xput {|p| [p.xid, p.key]}
    request_lock <= xget {|p| puts "try to get #{p.inspect}"; [p.xid, p.key]}

    goods <= lock_status {|s| puts "GD: #{s.inspect}"; [s.xid, s.resource] if s.status == :OK}
    xput_response <= (goods * xput_buf).lefts(:xid => :xid, :key => :key)
    #xget_response <= (goods * xget_buf).lefts(:xid => :xid)
    kvput <= (goods * xput_buf).rights(:xid => :xid, :key => :key) {|b| puts "PUT: #{b.inspect}"; [nil, b.key, b.xid, b.data]}
    kvget <= (goods * xget_buf).rights(:xid => :xid, :key => :key) {|b| [b.xid, b.key]}

    xput_buf <- (goods * xput_buf).rights(:xid => :xid, :key => :key)
    xget_buf <- (goods * xget_buf).rights(:xid => :xid, :key => :key)

    xget_response <= (kvget_response * xget_buf).pairs(:key => :key) do |r, b|
      puts "RESP!"
      [b.xid, r.key, r.value]
    end
  end 
end

