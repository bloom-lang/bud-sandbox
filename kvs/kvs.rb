require 'rubygems'
require 'bud'
require 'delivery/reliable_delivery'
require 'delivery/multicast'

module KVSProtocol
  include BudModule

  state do
    #interface input, :kvput, [:client, :key, :reqid] => [:value]
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

  declare 
  def mutate
    kvstate <+ kvput_internal.map{ |s|  [s.key, s.value] } 
    prev = join [kvstate, kvput_internal], [kvstate.key, kvput_internal.key]
    kvstate <- prev.map { |b, s| b }
  end

  declare
  def get
    getj = join([kvget, kvstate], [kvget.key, kvstate.key])
    kvget_response <= getj.map do |g, t|
      [g.reqid, t.key, t.value]
    end
  end

  declare
  def delete
    kvstate <- join([kvstate, kvdel], [kvstate.key, kvdel.key]).map {|s, d| s}
  end

  # place holder until scoping is fully implemented
  declare 
  def local_indir
    kvput_internal <= kvput
  end
end


module ReplicatedKVS
  include BasicKVS
  include MulticastProtocol

  #state {
  #  # override kvput
  #  interface input, :kvput_in, [:client, :key] => [:reqid, :value]
  #}

  declare
  def local_indir
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

