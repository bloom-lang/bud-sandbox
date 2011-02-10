require 'rubygems'
require 'test/unit'
require 'bfs/bfs_master'

class FSC < Bud
  include FS
end

b = FSC.new(:visualize => 3)

b.run_bg

b.sync_do{ b.fsls <+ [[123, '/']] }
b.sync_do{ assert_equal(2, b.

b.sync_do{ b.fsls <+ [[124, '/']] }


