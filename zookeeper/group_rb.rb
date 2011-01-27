require "rubygems"
require "zookeeper"

GROUP_NAME = "foo"
ZK_ADDR = "localhost:2181"

def mk_znode(z, path, ephemeral = false, sequence = false)
  r = z.create(:path => path, :ephemeral => ephemeral, :sequence => sequence)
  new_path = r[:path] ? r[:path] : path
  case r[:rc]
  when Zookeeper::ZOK then puts "Created '#{new_path}' successfully"
  when Zookeeper::ZNODEEXISTS then puts "Path '#{new_path}' exists already"
  else raise "Unknown error: #{r[:rc]}"
  end
end

z = Zookeeper.new(ZK_ADDR)
z.reopen
mk_znode(z, "/bud")
mk_znode(z, "/bud/#{GROUP_NAME}")
mk_znode(z, "/bud/#{GROUP_NAME}/member", true, true)

cb = Zookeeper::WatcherCallback.new {
  puts "Callback!"
  puts "Path: #{cb.context[:path]}"
}

z.get_children(:path => "/bud/#{GROUP_NAME}", :watcher => cb,
               :watcher_context => { :path => "foo" })

sleep 20
puts "Done!"
z.close
