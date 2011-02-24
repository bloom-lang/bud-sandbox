require 'rubygems'
require 'bud'
require 'bfs/chunking'
require 'bfs/bfs_master'
require 'bfs/datanode'

class DN
  include Bud
  include BFSDatanode
end


dn = DN.new(23456, {})
dn.add_member <+ [["localhost:12345", 1]]
dn.run
