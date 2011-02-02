require "rubygems"
require "bud"

path = ARGV[0] or raise "Usage: zk_create.rb path"

z = Zookeeper.new("localhost:2181")
r = z.delete(:path => path)
case r[:rc]
  when Zookeeper::ZOK:
    puts "Delete succeeded: #{path}"
  else
  raise "Error: #{z.zerror}"
end

z.close
