require 'rubygems'
require 'bud'
require 'bfs/chunking'
require 'bfs/bfs_master'
require 'bfs/datanode'
require 'bfs/background'

class Servy 
  include Bud
  include ChunkedKVSFS
  include BFSMasterServer
  include BFSBackgroundTasks
  include StaticMembership  
end

s = Servy.new(:port => 12345)
s.run
