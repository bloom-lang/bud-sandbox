require 'rubygems'
require 'bud'
require 'bfs/chunking'
require 'bfs/bfs_client'

c = BFSShell.new("localhost:12345")
c.run_bg

c.dispatch_command(ARGV)
sleep 1
