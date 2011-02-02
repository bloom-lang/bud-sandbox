require "rubygems"
require "bud"

class ZkTableTest < Bud
  def state
    zktable :foo, "/bat"
    #periodic :tik, 1
  end

  declare
  def other
    stdio <~ foo.map{|f| ["ZK: " + f.key + "," + f.value]}
    #foo <~ tik.map{|t| ["tik_" + t.time, "time = " + t.time]}
  end
end

z = ZkTableTest.new
z.run
