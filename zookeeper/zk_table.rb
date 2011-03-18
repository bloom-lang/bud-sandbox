require "rubygems"
require "bud"

class ZkTableTest < Bud
  def state
    zktable :foo, "/bat"
    #periodic :tik, 1
  end

  bloom do
    stdio <~ foo.map{|f| ["ZK: k = #{f.key}"]}
    stdio <~ foo.map{|f| ["ZK: k = " + f.key]}
    #foo <~ tik.map{|t| ["tik_" + t.val, "time = " + t.val]}
  end
end

z = ZkTableTest.new
z.run
