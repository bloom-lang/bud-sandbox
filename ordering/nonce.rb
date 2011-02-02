require 'rubygems'
require 'bud'
require 'membership/membership'

# one version of a nonce is a relation that is guaranteed to have only
# once unary tuple per timestep.  using it correctly to assign unique ids
# to arbitrary streams will require installing serializers
module NonceProto
  def state
    super
    interface input, :seed, []
    interface output, :nonce, ['ident']
  end
end

module GroupNonce
  include NonceProto
  include MembershipProto
  include Anise
  annotator :declare

  # a nonce generator built on top of a membership group.
  # at each timestep, return a number that is unique to
  # this host (among group members) and monotonically increasing

  def state
    super
    table :permo, [], ['ident']
    scratch :mcnt, [], ['cnt']
  end
  
  def bootstrap
    permo <= local_id
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
  include Anise
  annotator :declare
    
  # we don't need any state: just salt in address and local time.
  # assigning an arbitrary # of ids in a single timestep is another matter.

  # this is a technicality: need to drive things
  def state
    super
    table :permo, [], ['ident']
  end
  
  def bootstrap
    permo <= [[self.object_id << 16]]
  end

  declare
  def fiddle
    # ignore IP for now
    nonce <= permo.map{|p| @budtime.to_s or [p.ident + @budtime] }
    #nonce <= localtick.map{|l| [(@port << 16) + @budtime]  }
  end
end


# this works but is totally redundant with @budtime...!
module NNonce
  include NonceProto
  include Anise
  annotator :declare

  def state
    super
    table :storage, [], ['ident']
  end

  def bootstrap 
    storage <= [[0]]
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
  include Anise
  annotator :declare

  def state
    super
    scratch :storage, [], ['ident']
  end

  def bootstrap 
    storage <= [[0]]
  end

  declare 
  def logic
    nonce <= storage
    storage <+ storage.map {|s| puts "BUMP " + s.inspect or [s.ident + 1]}
  end
end
