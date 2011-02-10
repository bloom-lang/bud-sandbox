require 'rubygems'
require 'test/unit'
require 'bfs/fs_master'

class FSC < Bud
  include FS

  def bootstrap
    file <+ [[1, 'foo'], [2, 'bar']]
    dir <+ [[0, 1], [0, 2]]
    super
  end

end

class TestBFS < Test::Unit::TestCase
  def test_fsmaster
    b = FSC.new(:visualize => 3)

    b.run_bg

    b.sync_do{ b.fsls <+ [[123, '/']] }
    b.sync_do{ assert_equal(2, b.fsret.length) } 

    b.sync_do{ b.fsls <+ [[124, '/']] }
  end
end

