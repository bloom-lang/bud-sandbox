
class VectorClock

  def initialize
    @vector = {}
  end

  def [](client)
    if @vector.has_key?(client)
      return @vector[client]
    else
      return -1
    end
  end

  def initialize_copy(source)
    super
    @vector = @vector.dup
  end

  #define ordering based on maximum clock value in vector
  #this is somewhat arbitrary, but we need a tiebreaker and this seems reasonable
  def <=>(ov)
    return @vector.values.max <=> ov.get_max_clock
  end

  #does this vector "happen before" vector v?
  def happens_before(v)
    #need to ensure there is at least one element that is strictly less-than
    strictly_less = false

    for client in (v.get_clients+self.get_clients).uniq
      if (!@vector.has_key?(client) && v[client] != 0) || @vector[client] < v[client]
        strictly_less = true
      elsif @vector[client] > v[client]
        return false
      end
    end
    
    return strictly_less
  end

  #used for (non-strict) monotonic comparisons
  def happens_before_non_strict(v)
    #need to ensure there is at least one element that is strictly less-than
    for client in (v.get_clients+self.get_clients).uniq
      if @vector.has_key?(client) && @vector[client] > v[client]
        return false
      end
    end

    return true
  end

  def increment(client)
    check_client(client)
    @vector[client] += 1
  end

  #use with caution!
  def set_client(client, val)
    @vector[client] = val
  end

  def merge(v2)
    for client in v2.get_clients
      if !@vector.has_key?(client) || @vector[client] < v2[client]
        @vector[client] = v2[client]
      end
    end
  end

  #it'd be nice to make this protected, but we need to use it for
  #alternate consistency models like monotonic writes
  def get_clients
    return @vector.keys
  end

  private
  def check_client(client)
    if !@vector.has_key?(client)
        @vector[client] = 0
    end
  end

  protected
  def get_max_clock
    return @vector.values.max
  end

end
