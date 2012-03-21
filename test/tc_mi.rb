require './test_common'
require 'cache_coherence/mi/mi_cache'


class DumbDir
  include Bud
  include MIProtocol

  bloom do
    dcp_REXD <~ cdq_REX {|r| [r.cache_id, r.directory_id, r.line_id, r.payload]}
  end
end

class MICacheServer
  include Bud
  include MICache

  bloom do
    stdio <~ dcp_REXD.inspected
    stdio <~ cdq_REX_buf.inspected
    stdio <~ cpu_load.inspected
    #stdio <~ cache.inspected
    #stdio <~ lines.inspected
  end 
end

class TestMICache < Test::Unit::TestCase
  def test_cache1
    dir = DumbDir.new(:port => 12345, :trace => true)
    dir.run_bg
    cx = MICacheServer.new(:trace => true, :port => 64532)
    cx.directory << ['localhost:12345']
    cx.run_bg
    cx.sync_do {}

    cx.sync_do { cx.cpu_load <+ [['a', 1]] }
    cx.sync_do {}
  end
end
