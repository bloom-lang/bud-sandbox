require 'rubygems'
require 'bud'
require 'membership/membership'

# one version of a nonce is a relation that is guaranteed to have only
# one unary tuple per timestep.  using it correctly to assign unique ids
# to arbitrary streams will require installing serializers
module NonceProto
  include BudModule

  state {
    interface input, :seed, []
    interface output, :nonce, [] => [:ident]
  }
end

module GroupNonce
  include NonceProto
  include MembershipProto

  # a nonce generator built on top of a membership group.
  # at each timestep, return a number that is unique to
  # this host (among group members) and monotonically increasing

  state {
    table :permo, [] => [:ident]
    scratch :mcnt, [] => [:cnt]
  }

  def bootstrap
    permo <= local_id
    super
  end

  declare
  def fiddle
    mcnt <= member.group(nil, count)
    nonce <= join([permo,  mcnt]).map{ |p, m| [p.ident + (@budtime * m.cnt)] }
    permo <= join([seed, local_id]).map {|s, l| l if @budtime == 0 }
  end
end

module SimpleNonce
  include NonceProto

  # we don't need any state: just salt in address and local time.
  # assigning an arbitrary # of ids in a single timestep is another matter.

  # this is a technicality: need to drive things
  state {
    table :permo, [] => [:ident]
  }

  def bootstrap
    permo <= [[self.object_id << 16]]
    super
  end

  declare
  def fiddle
    # ignore IP for now
    nonce <= permo.map{|p| [p.ident + @budtime] }
    #nonce <= localtick.map{|l| [(@port << 16) + @budtime]  }
  end
end


# this works but is totally redundant with @budtime...!
module NNonce
  include NonceProto

  state {
    table :storage, [], [:ident]
  }

  def bootstrap
    storage <= [[0]]
    super
  end

  declare
  def logic
    storage <+ storage.map {|s| [s.ident + 1]}
    storage <- storage
    nonce <= storage
  end
end

# I thought the below would work
module SNNonce
  include NonceProto

  state {
    scratch :storage, [], [:ident]
  }

  def bootstrap
    storage <= [[0]]
    super
  end

  declare
  def logic
    nonce <= storage
    storage <+ storage.map {|s| puts "BUMP " + s.inspect or [s.ident + 1]}
  end
end
