require 'rubygems'
require 'bud'
require 'bfs/chunking'
require 'bfs/bfs_master'
require 'bfs/datanode'
require 'bfs/background'

s = BFSMasterServer.new(:port => 12345)
s.run
