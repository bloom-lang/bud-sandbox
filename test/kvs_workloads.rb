require 'rubygems'
require 'bud'
require 'test/unit'

require 'kvs/kvs'


module KVSWorkloads

  def add_members(b, *hosts)
    hosts.each do |h|
      #print "ADD MEMBER: #{h.inspect}\n"
      assert_nothing_raised(RuntimeError) { b.add_member <+ [[h]] }
    end
  end

  def workload1(v)
    v.sync_do { 
      v.kvput <+ [["localhost:54321", "foo", 1, "bar"]] 
    }
    #v.sync_do { v.kvput.each {|k| puts "K #{k.inspect}" }  }
    v.sync_do { v.kvput <+ [["localhost:54321", "foo", 2, "baz"]] }
    v.sync_do { v.kvput <+ [["localhost:54321", "foo", 3, "bam"]] }
    v.sync_do { v.kvput <+ [["localhost:54321", "foo", 4, "bak"]] }
    # give the messages a moment to arrive
    sleep 1
  end

  def workload2(v)
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 1, "bar"]] }
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 2, "baz"]] }
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 3, "bam"]] } 
    v.async_do{ v.kvput <+ [["localhost:54321", "foo", 4, "bak"]] }
    v.sync_do{ }
    #v.sync_do { v.kvstate.each{|k| puts "KVP: #{k.inspect}" } } 
  end

  def append(prog, item)
    curr = prog.bigtable.first[1]
    new = curr.clone
    new.push(item)
    prog.sync_do{ prog.kvput <+ [[ "localhost:54321", "foo", @id, new ]]  }
    @id = @id + 1
  end

  def workload3(v)
    v.sync_do{ v.kvput <+ [[ "localhost:54321", "foo", 1, ["bar"] ]] }
    assert_equal(1, v.bigtable.length)
    assert_equal("foo", v.bigtable.first[0])
    curr = v.bigtable.first[1]

    v.sync_do{ kvput <+ [[ "localhost:54321", "foo", 2, Array.new(curr).push("baz") ]] }
    assert_equal("foo", v.bigtable.first[0])
    assert_equal(['bar','baz'], v.bigtable.first[1])
  
    @id = 3 
    append(v, "qux")
    curr = v.bigtable.first[1]
    print "CURR is now #{curr.inspect}\n"
    append(v, "baq")
    print "CURR is now #{curr.inspect}\n"
    append(v, "raz")
    print "CURR is now #{curr.inspect}\n"
  end
end

