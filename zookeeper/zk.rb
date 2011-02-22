require "rubygems"
require "bud"
require "zookeeper"

class ZooMember < Bud
  ZK_ADDR = "localhost:2181"

  def initialize(zk_group)
    super()
    @group = zk_group
    @z = Zookeeper.new(ZK_ADDR)
  end

  state do
  end

  bootstrap do
  end

  declare
  def logic
  end
end

z = ZooMember.new("foo")
z.run
