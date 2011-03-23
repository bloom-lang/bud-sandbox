require 'rubygems'
require 'bud'
require 'membership/membership'

# one version of a nonce is a relation that is guaranteed to have only
# one unary tuple per timestep.  using it correctly to assign unique ids
# to arbitrary streams will require installing serializers
module NonceProto
  state do
    interface output, :nonce, [] => [:ident]
  end
end

module GroupNonce
  include NonceProto
  include MembershipProto

  # a nonce generator built on top of a membership group.
  # at each timestep, return a number that is unique to
  # this host (among group members) and monotonically increasing

  state do
    interface input, :seed, []
    table :permo, [] => [:ident]
    scratch :mcnt, [] => [:cnt]
  end

  bootstrap do
    permo <= local_id
  end

  bloom do
    mcnt <= member.group(nil, count)
    nonce <= join([permo,  mcnt]).map{ |p, m| [p.ident + (@budtime * m.cnt)] }
    permo <= join([seed, local_id]).map {|s, l| l if @budtime == 0 }
  end
end

module TimestepNonce
  include NonceProto

  # we don't need any state: just salt in address and local time.
  # assigning an arbitrary # of ids in a single timestep is another matter.

  # this is a technicality: need to drive things
  state do
    table :permo, [] => [:ident]
  end

  bootstrap do
    permo <= [[Time.new.to_i << 16]]
  end

  bloom do
    # ignore IP for now
    nonce <= permo.map{|p| [p.ident + @budtime] }
    #nonce <= localtick.map{|l| [(@port << 16) + @budtime]  }
  end
end


# this works but is totally redundant with @budtime...!
module NNonce
  include NonceProto

  state do
    table :storage_tab, [], [:ident]
  end

  bootstrap do
    storage_tab <= [[0]]
  end

  bloom do
    storage_tab <+ storage_tab.map {|s| [s.ident + 1]}
    storage_tab <- storage_tab
    nonce <= storage_tab
  end
end

# I thought the below would work
module SNNonce
  include NonceProto

  state do
    scratch :storage_tab, [], [:ident]
  end

  bootstrap do
    storage_tab <= [[0]]
  end

  bloom do
    nonce <= storage_tab
    storage_tab <+ storage_tab.map {|s| puts "BUMP " + s.inspect or [s.ident + 1]}
  end
end
