require "rubygems"
require "bud"
require "zookeeper"

class ZooMember < Bud
  ZK_ADDR = "localhost:2181"

  def initialize(ip, port, group)
    super(ip, port)
    @group = group
    @z = ZooKeeper.new(ZK_ADDR)
  end

  def state
    
  end

  def bootstrap
  end

  declare
  def logic
  end
end

z = ZooMember.new("localhost", 5555, "foo")
z.run
