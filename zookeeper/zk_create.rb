require "rubygems"
require "bud"

path = ARGV[0] or raise "Usage: zk_create.rb path [data]"
data = ARGV[1] or ""

z = Zookeeper.new("localhost:2181")
r = z.create(:path => path, :data => data)
case r[:rc]
  when Zookeeper::ZNODEEXISTS:
    puts "Node exists: #{path}"
  when Zookeeper::ZOK:
    puts "Create succeeded: #{path}"
  else
  raise "Error: #{z.zerror}"
end

z.close
