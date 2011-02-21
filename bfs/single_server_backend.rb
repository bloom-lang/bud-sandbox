require 'rubygems'
require 'bud'
require 'bfs/chunking'
require 'bfs/bfs_master'
require 'bfs/datanode'

class Servy 
  include Bud
  include ChunkedKVSFS
  include BFSMasterServer
  include StaticMembership  
end

class DN
  include Bud
  include BFSDatanode
end


dn = DN.new(23456, {})
dn.add_member <+ [["localhost:12345", 1]]
dn.run_bg

s = Servy.new(:port => 12345)
s.run_bg

while true do
  sleep 1
end

