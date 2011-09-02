
class VectorClock

  def initialize
    @vector = {}
  end

  def [](client)
    check_client(client)
    return @vector[client]
  end

  def increment(client)
    check_client(client)
    @vector[client] += 1
  end

  def merge(v2)
    for client in v2.get_clients
      if !@vector.has_key?(client) or @vector[client] < v2[client]
        @vector[client] = v2[client]
      end
    end
  end

  private
  def check_client(client)
    if !@vector.has_key?(client):
        @vector[client] = 0
    end
  end

  protected
  def get_clients
    return @vector.keys
  end

end
