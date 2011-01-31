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

  def state
    
  end

  def bootstrap
  end

  declare
  def logic
  end
end

z = ZooMember.new("foo")
z.run
