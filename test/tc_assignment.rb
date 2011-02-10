require 'rubygems'
require 'test/unit'
require 'bud'
require 'ordering/assigner'

class AS < Bud
  include AggAssign
end

class TestAssg < Test::Unit::TestCase
  def test_assigner
    as = AS.new
    as.run_bg
    as.async_do{ as.dump <+ [['foobar'], ['blimblam'], ['fizzbuzz']] }
    as.sync_do{ 
      assert_equal(3, as.pickup.length)
      id = -1
      as.pickup.each do |p|
        assert(!(id == p.ident))
        id = p.ident
      end      
    }
  end
end
