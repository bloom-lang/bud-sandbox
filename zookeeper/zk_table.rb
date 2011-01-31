require "rubygems"
require "bud"

class ZkTableTest < Bud
  def state
    zktable :foo, "/foo"
  end

  declare
  def other
  end
end

z = ZkTableTest.new
z.run
