module MIProtocol
  state do
    # schema templates
    #channel :cd_template, [:@directory_id, :cache_id, :line_id]
    #channel :dc_template, [:@cache_id, :directory_id, :line_id]

    # channels shared by agents
    channel :cdq_REX, [:@directory_id, :cache_id, :line_id]  # cd_template
    # really, want to make :payload functionally dependent on the other 3 cols.
    channel :cdq_WBD, [:@directory_id, :cache_id, :line_id, :payload] #cd_template + :payload
    channel :dcp_REXD, [:@cache_id, :directory_id, :line_id, :payload]     # dc_template + :payload
    channel :dcp_WBAK, [:@cache_id, :directory_id, :line_id] # dc_template
    channel :dcp_NAK, [:@cache_id, :directory_id, :line_id] # dc_template
    channel :dcq_INV, [:@cache_id, :directory_id, :line_id] # dc_template
    channel :cdp_INVD, [:@directory_id, :cache_id, :line_id, :payload] #cd_template + :payload

    # EDB/a priori truth
    # necessary for bootstrapping states of lines, caches.
    table :lines, [:line]
    table :caches, [:cache_id]
    table :directory, [:directory_id]
  end

  bootstrap do
    lines <= [
      [1],
      [2],
      [3],
      [4],
      [5],
      [6],
      [7]
    ];
    caches <= [
      ['a'],
      ['b'],
      ['c'],
      ['d'],
    ];
  end
end
