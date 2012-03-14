module KVSWorkloads
  def add_members(b, *hosts)
    hosts.each_with_index do |h, i|
      #puts "ADD MEMBER: #{h.inspect}"
      b.add_member <+ [[i, h]]
    end
  end

  def workload1(v)
    v.sync_do { 
      v.kvput <+ [["localhost:54321", "foo", 1, "bar"]] 
    }
    v.sync_do { v.kvput <+ [["localhost:54321", "foo", 2, "baz"]] }
    v.sync_do { v.kvput <+ [["localhost:54321", "foo", 3, "bam"]] }
    v.sync_do { v.kvput <+ [["localhost:54321", "foo", 4, "bak"]] }
    v.tick; v.tick
  end

  def workload2(v)
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 1, "bar"]] }
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 2, "baz"]] }
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 3, "bam"]] } 
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 4, "bak"]] }
    v.sync_do
  end
end
