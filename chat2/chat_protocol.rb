module ChatProtocol
  state do
    channel :sent
    channel :recieved
    channel :connect
  end

  DEFAULT_ADDR = "localhost:12345"
end
