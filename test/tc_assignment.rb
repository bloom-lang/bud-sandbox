require './test_common'
require 'ordering/assigner'

class AS
  include Bud
  include AggAssign
end

class TestAssg < Test::Unit::TestCase
  def test_assigner
    as = AS.new
    as.run_bg
    as.async_do{ as.id_request <+ [['foobar'], ['blimblam'], ['fizzbuzz']] }
    as.sync_do{
      assert_equal(3, as.id_response.length)
      id = -1
      as.id_response.each do |p|
        assert(!(id == p.ident))
        id = p.ident
      end
    }
  end
end
