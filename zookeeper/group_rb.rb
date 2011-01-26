require "rubygems"
require "zookeeper"

GROUP_NAME = "foo"
ZK_ADDR = "localhost:2181"

z = Zookeeper.new(ZK_ADDR)
r = z.create(:path => "/bud")
case r[:rc]
  when Zookeeper::ZOK then puts "Created '/bud' successfully"
  when Zookeeper::ZNODEEXISTS then puts "Path '/bud' exists already"
  else puts "Unknown error: #{r[:rc]}"
end

z.close
