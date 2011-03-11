require 'rubygems'
require 'bud'
require 'bfs/chunking'
require 'bfs/bfs_client'

c = BFSShell.new("localhost:12345")
c.run_bg

res = c.dispatch_command(ARGV)


puts "Result:\n#{res.inspect}"

