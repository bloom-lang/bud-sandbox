require 'rubygems'
require 'bud'
require 'test/unit'

require 'kvs/kvs'


module KVSWorkloads

  def add_members(b, *hosts)
    hosts.each do |h|
      #print "ADD MEMBER: #{h.inspect}\n"
      b.add_member <+ [[h]]
    end
  end

  def workload1(v)
    v.sync_do { 
      v.kvput <+ [["localhost:54321", "foo", 1, "bar"]] 
    }
    v.sync_do { v.kvput <+ [["localhost:54321", "foo", 2, "baz"]] }
    v.sync_do { v.kvput <+ [["localhost:54321", "foo", 3, "bam"]] }
    v.sync_do { v.kvput <+ [["localhost:54321", "foo", 4, "bak"]] }
  end

  def workload2(v)
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 1, "bar"]] }
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 2, "baz"]] }
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 3, "bam"]] } 
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 4, "bak"]] }
    v.sync_do{ }
  end
end

