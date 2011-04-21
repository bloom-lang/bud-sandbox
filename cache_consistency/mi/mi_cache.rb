require 'rubygems'
require 'bud'
require 'mi_protocol'

module MICPUInterface
  state do
    interface input, :cpu_load, [:cache, :line]
    interface input, :cpu_store, [:cache, :line, :data]
    interface output, :load_resp, [:cache, :line, :data]
  end
end

module MICache
  include MIProtocol
  include MICPUInterface

  state do
    table :cache, [:line] => [:data]
    table :state, [:line] => [:state]

    table :cdq_REX_buf, cdq_REX.schema
  end

  bootstrap do
    # to keep things simple, we'll just create the whole cache, and update its 
    # cells when necessary
    cache << [1, nil]
    cache << [2, nil]
    cache << [3, nil]
    cache << [4, nil]
    cache << [5, nil]
    cache << [6, nil]
  
    state << [1, nil]
    state << [2, nil]
    state << [3, nil]
    state << [4, nil]
    state << [5, nil]
    state << [6, nil]
  
  end

  bloom do
    temp (:load_info) <= (cpu_load * state * cache * directory).combos(cpu_load.line => state.line)

    temp :do_rex <= load_info do |l, s, c, d|
      puts "ROW FROM LI"
      if s.state == :csINV
          [d.directory, l.cache, l.line]
      end
    end

    cdq_REX <~ do_rex
    cdq_REX_buf <= do_rex

    # perhaps the line is in cache...
    #@load_resp <= (load_info * cache).combos(load_info.cpu_load.line => cache.line) do |l, s, d, c|
    load_resp <= load_info do |l, s, c, d|
      if s.state == :csEXC
        [l.cache, l.line, c.data]
      end
    end

    # otherwise, when we get it back...
    temp :fresh_cell <= (dcp_REXD * cdq_REX_buf).pairs(dcp_REXD.line_id => cdq_REX_buf.line_id)
    cache <+ fresh_cell {|c| [c.line, c.payload]}
    load_resp <= fresh_cell {|c| [c.cache, c.line, c.payload]}

    # [..]

    temp :upd1 <= (dcp_REXD * state).pairs(dcp_REXD.line_id => state.line)
    state <+ upd1 do |r,s|
      if s.state == csINV
        [r.line, :csEXC]
      end
    end
    state <- upd1 {|r, s| s}

    temp :inf_info <= (dcq_INV * state * directory).combos(dcq_INV.line_id => state.line)

    cdp_INVD <~ inf_info.combos {|i, s, d| [d.directory, i.cache_id, i.line_id]}
    state <+ inf_info.combos {|i, s, d| [s.line, :csBSY]}
    state <- inf_info.combos {|i, s, d| s}
   
    temp :upd2 <= (dcp_WBAK * state).pairs(dcp_WBAK.line_id => state.line)

    state <+ upd2.pairs {|a, s| [s.line, :csINV]}
    state <- upd2 {|a, s| s}
  end
end
