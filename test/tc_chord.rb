require './test_common'
require 'chord/chord_node'
require 'chord/chord_find'
require 'chord/chord_join'
require 'chord/chord_stable'
require 'chord/chord_successors'

module LilChord
  import ChordFind => :finder
  
  def initialize(opts)
    @addrs = {0 =>'127.0.0.1:12340', 1 => '127.0.0.1:12341', 3 => '127.0.0.1:12343'}
    @maxkey = 8
    super
  end

  def do_lookups
    # issue local lookups for 1,2,6
    [1,2,6].each do |num|
      finder.pred_req <+ [[num]]
      finder.succ_req <+ [[num]]
    end
  end
  
  def load_data
    mod = @options[:port].to_i%10
    if mod == 0
      me <+ [[0, 3, '127.0.0.1:12343']]
      finger <+ [[0,1,2,1,@addrs[1]], [1,2,4,3,@addrs[3]], [2,4,0,0,@addrs[0]]]
      localkeys <+ [[6, '']]
    elsif mod == 1
      me <+ [[1, 0, '127.0.0.1:12340']]
      finger <+ [[0,2,3,3,@addrs[3]], [1,3,5,3,@addrs[3]], [2,5,1,0,@addrs[0]]]
      localkeys <+ [[1, '']]
    elsif mod == 3
      me <+ [[3, 1, '127.0.0.1:12341']]
      finger <+ [[0,4,5,0,@addrs[0]], [1,5,7,0,@addrs[0]], [2,7,3,0,@addrs[0]]]
      localkeys <+ [[2, '']]
    end
  end
  
  def do_join
    stdio <~ [['sending to 127.0.0.1:12340']]
    join_req <~ [['127.0.0.1:12340', '127.0.0.1:12346', 6]]
  end
 
  bootstrap do
    stdio <~ [["bootstrapped #{ip_port}"]]
  end
  
  state do
    table :succ_cache, finder.succ_resp.schema
    table :pred_cache, finder.pred_resp.schema
  end

  bloom :persist_resps do
    succ_cache <= finder.succ_resp
    pred_cache <= finder.pred_resp
  end
end

class LilChordJoin
  include Bud
  include ChordNode
  include LilChord
  include ChordJoin
  include ChordSuccessors
end

class LilChordStable
  include Bud
  include ChordNode
  include LilChord
  include ChordStabilize
  include ChordSuccessors
end

class TestChord < MiniTest::Unit::TestCase
  def do_lookup_tests(nodes)
    p = Queue.new
    q = Queue.new
  
    nodes.each do |n|
      n.register_callback(:finder__pred_resp) { |pc| p.push([pc.bud_instance.port, pc.first.key, pc.first.start]) }
      n.register_callback(:finder__succ_resp) { |sc| q.push([sc.bud_instance.port, sc.first.key, sc.first.start]) }
    end
  
    nodes.each{|n| n.sync_do {n.do_lookups}}
    [p,q].each do |queue|
      ha = {}
      while ha.keys.size < 3 or ha.map{|k,v| v.length}.uniq != [3]
        print "."
        it = queue.pop
        ha[it[0]] ||= []
        ha[it[0]] << [it[1], it[2]]
        ha[it[0]].uniq!
      end 
    end  
    
    # now check that all nodes hear the same results for pred_resp and succ_resp.
    # we check for agreement pairwise.
    (0..nodes.length - 2).each do |i|
      assert_equal(3, nodes[i].pred_cache.length)
      assert_equal(nodes[i].pred_cache.to_a.sort, nodes[i+1].pred_cache.to_a.sort)  
      assert_equal(3, nodes[i].succ_cache.length)
      assert_equal(nodes[i].succ_cache.to_a.sort, nodes[i+1].succ_cache.to_a.sort)
    end
  end
  
  def do_joined_ring_tests(nodes)
    nodes.each { |n| n.sync_do }
    assert_equal([[[0, 6, "127.0.0.1:12346"]], 
                  [[1, 0, "127.0.0.1:12340"]], 
                  [[3, 1, "127.0.0.1:12341"]], 
                  [[6, 3, "127.0.0.1:12343"]]],
                 nodes.map{|n| n.me.to_a})   
  end
  
  def do_joined_finger_tests(nodes)
    status = 0
    wait = 0
    begin
      nodes.each{|n| n.sync_do }
      sleep 1
      print "."
      status = 1
      status = 0 unless [[0, 7, 0, 0, "127.0.0.1:12340"],
                         [1, 0, 2, 0, "127.0.0.1:12340"],
                         [2, 2, 6, 3, "127.0.0.1:12343"]] \
                        == nodes[3].finger.to_a.sort
      status = 0 unless [[0, 1, 2, 1, "127.0.0.1:12341"],
                         [1, 2, 4, 3, "127.0.0.1:12343"],
                         [2, 4, 0, 6, "127.0.0.1:12346"]] \
                        == nodes[0].finger.to_a.sort
      status = 0 unless [[0, 2, 3, 3, "127.0.0.1:12343"],
                         [1, 3, 5, 3, "127.0.0.1:12343"],
                         [2, 5, 1, 6, "127.0.0.1:12346"]] \
                        == nodes[1].finger.to_a.sort
      status = 0 unless [[0, 4, 5, 6, "127.0.0.1:12346"], 
                         [1, 5, 7, 6, "127.0.0.1:12346"],
                         [2, 7, 3, 0, "127.0.0.1:12340"]] \
                        == nodes[2].finger.to_a.sort
      wait += 1
    end while status == 0 and wait < 20
    
    assert_equal(       [[0, 7, 0, 0, "127.0.0.1:12340"],
                         [1, 0, 2, 0, "127.0.0.1:12340"],
                         [2, 2, 6, 3, "127.0.0.1:12343"]] \
                        , nodes[3].finger.to_a.sort)
    assert_equal(       [[0, 1, 2, 1, "127.0.0.1:12341"],
                         [1, 2, 4, 3, "127.0.0.1:12343"],
                         [2, 4, 0, 6, "127.0.0.1:12346"]] \
                        , nodes[0].finger.to_a.sort)
    assert_equal(       [[0, 2, 3, 3, "127.0.0.1:12343"],
                         [1, 3, 5, 3, "127.0.0.1:12343"],
                         [2, 5, 1, 6, "127.0.0.1:12346"]] \
                        , nodes[1].finger.to_a.sort)
    assert_equal(       [[0, 4, 5, 6, "127.0.0.1:12346"], 
                         [1, 5, 7, 6, "127.0.0.1:12346"],
                         [2, 7, 3, 0, "127.0.0.1:12340"]] \
                        , nodes[2].finger.to_a.sort)
  end

  def do_multiple_successor_tests(nodes)
    status = 0
    wait = 0
    begin
      sleep 1
      print "."
      status = 1
      status = 0 unless [[1, 1, "127.0.0.1:12341", 3], 
                         [2, 3, "127.0.0.1:12343", 5]] \
                        == nodes[3].successors.to_a.sort
      status = 0 unless [[1, 3, "127.0.0.1:12343", 3], 
                         [2, 6, "127.0.0.1:12346", 6]] \
                        == nodes[0].finger.to_a.sort
      status = 0 unless [[1, 6, "127.0.0.1:12346", 5], 
                         [2, 0, "127.0.0.1:12340", 7]] \
                        == nodes[1].successors.to_a.sort
      status = 0 unless [[1, 0, "127.0.0.1:12340", 5], 
                         [2, 1, "127.0.0.1:12341", 6]] \
                        == nodes[2].successors.to_a.sort
      wait += 1
    end while status == 0 and wait < 120
    
    assert_equal(       [[1, 1, "127.0.0.1:12341", 3], 
                         [2, 3, "127.0.0.1:12343", 5]] \
                        , nodes[3].successors.to_a.sort)
    assert_equal(       [[1, 3, "127.0.0.1:12343", 3], 
                         [2, 6, "127.0.0.1:12346", 6]] \
                        , nodes[0].successors.to_a.sort)
    assert_equal(       [[1, 6, "127.0.0.1:12346", 5], 
                         [2, 0, "127.0.0.1:12340", 7]] \
                        , nodes[1].successors.to_a.sort)
    assert_equal(       [[1, 0, "127.0.0.1:12340", 5], 
                         [2, 1, "127.0.0.1:12341", 6]] \
                        , nodes[2].successors.to_a.sort)
  end

  def no_test_find
    STDOUT.sync = true
    puts "beginning static find test"
    ports = [12340, 12341, 12343]
    my_nodes = ports.map do |p|
      LilChordJoin.new(:port => p, :metrics => true)#, :dump_rewrite=>true)
    end
    my_nodes.each{|n| n.run_bg}
    my_nodes.each{|n| n.sync_do {n.load_data}}

    print "checking lookups"
    begin
      do_lookup_tests(my_nodes)
    rescue Test::Unit::AssertionFailedError
      my_nodes.each{|n| n.stop}
      raise
    end
    puts "done"
    my_nodes.each{|n| n.stop}
  end
    
  def no_test_join
    STDOUT.sync = true
    
    puts "beginning node join test"
    @addrs = {0 => 12340, 1 => 12341, 3 => 12343}
    @my_nodes = @addrs.values.map do |a|
      LilChordJoin.new(:port => a)#, :trace => true, :tag => "node#{a}")
    end
    @my_nodes.each{|n| n.run_bg}
    @my_nodes.each{|n| n.sync_do {n.load_data}}
    
    @addrs[6] = 12346
    newnode = LilChordJoin.new(:port => 12346) #, :metrics => true)#, :trace => true, :tag => "node12346")
    @my_nodes << newnode
    newnode.run_bg
    newnode.sync_do{newnode.me <+ [[6, nil, nil]]}
  
    p = Queue.new
    q = Queue.new
    xq = Queue.new
    nfq = Queue.new
    @my_nodes.each do |n|
      n.register_callback(:succ_upd_pred) { |s| p.push([n.me.first.start, s.first.pred_id]) }
      n.register_callback(:node_pred_resp) { |f| p.push([n.me.first.start, f.first.pred_id]) }
      n.register_callback(:finder__succ_resp) { |sc| q.push([sc.bud_instance.port, sc.to_a]) }
      n.register_callback(:new_finger) { |f| nfq.push([f.bud_instance.port, f.to_a]) }
     end    
    # @my_nodes[0].register_callback(:xfer_keys_ack) {|x| xq.push(x.first)}
    
    newnode.sync_do{newnode.join_up <+ [['127.0.0.1:12340', 6]]}
         
    begin
      # check ring of predecessors
      print "checking ring"
      results = []
      while results.length < 2
        it = p.pop
        print "."
        if it[0] == 0 or it[0] == 6
          # puts it.inspect
          results << it 
          results.uniq!
        end
      end   
      do_joined_ring_tests(@my_nodes)
      puts "done"
                 
      # check localkeys
      print "checking localkeys"
      # xqout = xq.pop
      print "."
      # puts "got #{xqout.inspect}"
      # @my_nodes.each { |n| n.sync_do }
      assert_equal([["127.0.0.1:12340", []], 
                    ["127.0.0.1:12341", [[1, ""]]], 
                    ["127.0.0.1:12343", [[2, ""]]], 
                    ["127.0.0.1:12346", [[6, ""]]]],
                   @my_nodes.map{|n| [n.ip_port, n.localkeys.to_a]})
      puts "done"
  
      print "checking fingers"
      do_joined_finger_tests(@my_nodes)
      puts "done"
                 
      if @my_nodes[0].respond_to?(:successors)
        print "checking multiple successors"
        do_multiple_successor_tests(@my_nodes)
        puts "done"
      end
      
      # check lookup consistency
      print "checking lookups"
      do_lookup_tests(@my_nodes)
    rescue Test::Unit::AssertionFailedError
      @my_nodes.each{|n| n.stop}
      raise
    end
    puts "done"
    @my_nodes.each{|n| n.stop}
  end
    
  def test_stabilize
    # allow us to see status messages mid-line
    STDOUT.sync = true
    
    puts "beginning test of join via stabilization"
    @addrs = {0 => 12340, 1 => 12341, 3 => 12343}
    @my_nodes = @addrs.values.map do |a|
      LilChordStable.new(:port => a, :metrics=>true)# :trace => true, :tag => "node#{a}")
    end
    @my_nodes.each{|n| n.run_bg}
    @my_nodes.each{|n| n.sync_do {n.load_data}}
    @addrs[6] = 12346
    newnode = LilChordStable.new(:port => 12346, :metrics => true)#, :trace => true, :tag => "node12346")
    @my_nodes << newnode
    newnode.run_bg
    
    predq = Queue.new
    newnode.register_callback(:succ_notify) { |s| predq.push(1) }
    xferq = Queue.new
    @my_nodes[0].register_callback(:xfer_keys_ack) { |x| xferq.push(1) }
    
    newnode.sync_do{newnode.me <+ [[6, nil, nil]]}
  
    newnode.sync_do{newnode.join_up <+ [['127.0.0.1:12340', 6]]}
    
    begin
      # after join, check predecessors in ring
      print "checking ring"
      predq.pop
      print "--"
      do_joined_ring_tests(@my_nodes)
      puts "done"
                 
      # check localkeys
      print "checking localkeys"
      xferq.pop
      print "--"
      # @my_nodes.each { |n| n.sync_do }
      assert_equal([["127.0.0.1:12340", []], 
                    ["127.0.0.1:12341", [[1, ""]]], 
                    ["127.0.0.1:12343", [[2, ""]]], 
                    ["127.0.0.1:12346", [[6, ""]]]],
                   @my_nodes.map{|n| [n.ip_port, n.localkeys.to_a]})
      puts "done"
  
      print "waiting for finger updates"
      do_joined_finger_tests(@my_nodes)
      puts "done"
      
      if @my_nodes[0].respond_to?(:successors)
        print "checking multiple successors"
        do_multiple_successor_tests(@my_nodes)
        puts "done"
      end
                 
      # check lookup consistency
      print "checking lookups"
      do_lookup_tests(@my_nodes)
    rescue Test::Unit::AssertionFailedError
      @my_nodes.each{|n| n.stop}
      raise
    end
    puts "done"
    
    # # now stop node 1 and check result of stabilization again
    # # clear xferq
    # (1..xferq.length).each { xferq.pop }
    # @my_nodes[1].stop
    # # check localkeys
    # print "checking localkeys after 1 leaves"
    # x = xferq.pop
    # puts "x is #{x.inspect}"
    # print "--"
    # @my_nodes.each_with_index { |n,i| puts "#{n.ip_port} keys: #{n.localkeys.to_a.inspect}" unless i == 1 }
    # @my_nodes.each_with_index { |n,i| puts "#{n.ip_port} fingers: #{n.finger.to_a.inspect}" unless i == 1 }
    # # assert_equal([["127.0.0.1:12340", []], 
    # #               ["127.0.0.1:12341", [[1, ""]]], 
    # #               ["127.0.0.1:12343", [[2, ""]]], 
    # #               ["127.0.0.1:12346", [[6, ""]]]],
    # #              @my_nodes.map{|n| [n.ip_port, n.localkeys.to_a]})
    # puts "done"    
    
    @my_nodes.each{|n| n.stop}
  end
end
