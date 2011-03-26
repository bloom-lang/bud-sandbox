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
    #interface input, :kvput_internal, [:client, :key] => [:reqid, :value]
    scratch :kvput_internal, [:client, :key] => [:reqid, :value]
  end

  bloom :mutate do
    kvstate <+ kvput_internal.map {|s|  [s.key, s.value]}
    kvstate <- (kvstate * kvput_internal).lefts(:key => :key)
  end

  bloom :get do
    temp :getj <= join([kvget, kvstate], [kvget.key, kvstate.key])
    kvget_response <= getj.map do |g, t|
      [g.reqid, t.key, t.value]
    end
  end

  bloom :delete do
    kvstate <- (kvstate * kvdel).lefts(:key => :key)
  end

  # place holder until scoping is fully implemented
  bloom :local_indir do
    kvput_internal <= kvput
  end
end


module ReplicatedKVS
  include BasicKVS
  include MulticastProtocol

  #state do
  #  # override kvput
  #  interface input, :kvput_in, [:client, :key] => [:reqid, :value]
  #end

  bloom :local_indir do
    # if I am the master, multicast store requests
    send_mcast <= kvput.map do |k|
      unless member.include? [k.client]
        [k.reqid, [@addy, k.key, k.reqid, k.value]]
      end
    end

    kvput_internal <= mcast_done.map {|m| m.payload }

    # if I am a replica, store the payload of the multicast
    kvput_internal <= pipe_chan.map do |d|
      if d.payload.fetch(1) != @addy
        d.payload
      end
    end
  end
end

