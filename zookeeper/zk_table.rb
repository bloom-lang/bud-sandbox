require "rubygems"
require "bud"

class ZkTableTest < Bud
  def initialize(ip, port)
    super(ip, port)
  end

  def state
    zktable :z, "/"
  end

  declare
  def logic
    stdio <~ zktable.map{|z| z.value}
  end
end

z = ZkTableTest.new("localhost", 5000)
z.run
