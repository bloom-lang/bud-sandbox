require 'cache_coherence/mi/mi_protocol'

module MIDirectory
  include MIProtocol

  state do
    table :dsINV, [:line_id]
    table :dsEXC, [:line_id, :holder]
    table :dsBEX, [:line_id, :holder, :requester]
    table :cache, [:line_id] => [:value]
  end

  bloom  do
    # dsINV + cdqREX = dsEXC + dcpREXD
    dcp_REXD <~ (dsINV * cdq_REX * cache).matches do |_,r,c|
      [r.cache_id, r.directory_id, r.line_id, c.value]
    end
    # atomically transition to next state
    dsEXC <= (dsINV * cdq_REX).matches do |_,r,_|
      [r.line_id, r.cache_id]
    end
    dsINV <- (dsINV * cdq_REX).matches do |s,_|
      s
    end

    # dsEXC + cdqREX = dsBEX + dcqINV
    dcq_INV <~ (dsEXC * cdq_REX * cache).matches do |s,r,c|
      [s.holder, r.directory_id, s.line_id]
    end
    # atomically transition to next state
    dsBEX <= (dsEXC * cdq_REX).matches do |s,r|
      [s.line_id, s.holder, r.cache_id]
    end
    dsEXC <- (dsEXC * cdq_REX).matches do |s,_|
      s
    end

    # dsEXC + cdqWBD = dsINV + dcpWBAK + "update cache"
    dcp_WBAK <~ (dsEXC * cdq_WBD * cache).matches do |s,r,c|
      [s.holder, r.directory_id, r.line_id]
    end
    # atomically tarnsition to next state
    dsINV <= (dsEXC * cdq_WBD).matches do |s,_|
      [s.line_id]
    end
    dsEXC <- (dsEXC * cdq_WBD).matches do |s,_|
      s
    end
    # atomically update cache
    cache <= (dsEXC * cdq_WBD).matches do |_,r|
      [r.line_id, r.payload]
    end
    cache <- (dsEXC * cdq_WBD * cache).matches do |_,r,c|
      [r.line_id, c.value]
    end

    # dsBEX + cdq_REX = dcp_NAK
    dcp_NAK <~ (dsBEX * cdq_REX).matches do |_,r|
      [r.cache_id, r.directory_id, r.line_id]
    end

    # dsBEX + cdp_INVD = dsEXC + dcp_REXD + "update cache"
    dcp_REXD <~ (dsBEX * cdp_INVD).matches do |s,r|
      [s.requester, r.directory_id, s.line_id, r.payload]
    end
    # atomically transition to next state
    dsEXC <= (dsBEX * cdp_INVD).matches do |s,_|
      [s.line_id, s.requester] # the new holder is the previous requester
    end
    dsBEX <- (dsBEX * cdp_INVD).matches do |s,_|
      s
    end
    # atomically update cache
    cache <= (dsBEX * cdp_INVD).matches do |_,r|
      [r.line_id, r.payload]
    end
    cache <- (dsBEX * cdp_INVD * cache).matches do |_,r,c|
      [r.line_id, c.value]
    end

    # dsBEX + cdq_WBD = dsEXC + dcp_REXD + dcp_WBAK + "update cache"
    dcp_REXD <~ (dsBEX * cdq_WBD).matches do |s,r|
      [s.requester, r.directory_id, s.line_id, r.payload]
    end
    dcp_WBAK <~ (dsBEX * cdq_WBD).matches do |s,r|
      [s.holder, r.directory_id, s.line_id]
    end
    # atomically transition to next state
    dsEXC <= (dsBEX * cdq_WBD).matches do |s,_|
      [s.line_id, s.requester] # the new holder is the previous requester
    end
    dsBEX <- (dsBEX * cdq_WBD).matches do |s,_|
      s
    end
    # atomically update cache
    cache <= (dsBEX * cdq_WBD).matches do |_,r|
      [r.line_id, r.payload]
    end
    cache <- (dsBEX * cdq_WBD * cache).matches do |_,r,c|
      [r.line_id, c.value]
    end
  end
end
