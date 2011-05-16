require 'rubygems'
require 'bud'
require 'test/unit'
require 'chord/chord_node'
require 'chord/chord_find'
require 'chord/chord_join'

module LilChord
  import ChordFind => :finder

  def do_lookups
    # issue local lookups for 1,2,6
    [1,2,6].each do |num|
      finder.pred_req <+ [[num]]
      finder.succ_req <+ [[num]]
    end
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

class LilChordClass
  include Bud
  include ChordNode
  include LilChord
  include ChordJoin
  
  def initialize(opts)
    @addrs = {0 =>'127.0.0.1:12340', 1 => '127.0.0.1:12341', 3 => '127.0.0.1:12343'}
    @maxkey = 8
    super
  end
  
  bootstrap do
    stdio <~ [["bootstrapped #{ip_port}"]]
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
end

class TestFind < Test::Unit::TestCase
  def test_find
    ports = [12340, 12341, 12343]
    my_nodes = ports.map do |p|
      LilChordClass.new(:port => p)#, :dump_rewrite=>true)
    end
  
    p = Queue.new
    q = Queue.new
    fq = Queue.new
    my_nodes.each do |n|
      n.register_callback(:finder__pred_resp) { |pc| p.push([pc.bud_instance.port, pc.first.key, pc.first.start]) }
      n.register_callback(:finder__succ_resp) { |sc| q.push([sc.bud_instance.port, sc.first.key, sc.first.start]) }
    end
  
    my_nodes.each{|n| n.run_bg}
    my_nodes.each{|n| n.sync_do {n.load_data}}
    my_nodes.each{|n| n.sync_do {n.do_lookups}}
  
    # wait for pred_resp and succ_resp of length 3 on each of 3 nodes
    print "checking lookups"
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
    puts "done"
    my_nodes.each{|n| n.stop_bg}
    
    assert_equal(3, my_nodes[0].pred_cache.length)
    assert_equal(my_nodes[0].pred_cache.to_a.sort, my_nodes[1].pred_cache.to_a.sort)
    assert_equal(my_nodes[1].pred_cache.to_a.sort, my_nodes[2].pred_cache.to_a.sort)
  
    assert_equal(3, my_nodes[0].succ_cache.length)
    assert_equal(my_nodes[0].succ_cache.to_a.sort, my_nodes[1].succ_cache.to_a.sort)
    assert_equal(my_nodes[1].succ_cache.to_a.sort, my_nodes[2].succ_cache.to_a.sort)
    end
    
    def test_join
      # allow us to see status messages mid-line
      STDOUT.sync = true
      
      @addrs = {0 => 12340, 1 => 12341, 3 => 12343}
      @my_nodes = @addrs.values.map do |a|
        LilChordClass.new(:port => a)#, :trace => true, :tag => "node#{a}")
      end
      @my_nodes.each{|n| n.run_bg}
      @my_nodes.each{|n| n.sync_do {n.load_data}}
      @addrs[6] = 12346
      newnode = LilChordClass.new(:port => 12346)#, :trace => true, :tag => "node12346")
      @my_nodes << newnode
      newnode.run_bg
      newnode.sync_do{newnode.me <+ [[6, nil, nil]]}
      newnode.sync_do{newnode.join_up <+ [['127.0.0.1:12340', 6]]}
    
      p = Queue.new
      q = Queue.new
      xq = Queue.new
      ffq = Queue.new
      fuq = Queue.new
      @my_nodes.each do |n|
        n.register_callback(:succ_upd_pred) { |s| p.push([n.me.first.start, s.first.pred_id]) }
        n.register_callback(:node_pred_resp) { |f| p.push([n.me.first.start, f.first.pred_id]) }
        n.register_callback(:finder__succ_resp) { |sc| q.push([sc.bud_instance.port, sc.to_a]) }
        n.register_callback(:finger_upd) { |f| fuq.push([f.bud_instance.port, f.to_a]) }
       end    
      # @my_nodes[0].register_callback(:xfer_keys_ack) {|x| xq.push(x.first)}
      newnode.register_callback(:fix_finger_finder__succ_resp) {|f| ffq.push(f.to_a)} 
      
      newnode.sync_do{newnode.join_up <+ [['127.0.0.1:12340', 6]]}
           
      #     
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
      @my_nodes.each { |n| n.sync_do }
      assert_equal([[[0, 6, "127.0.0.1:12346"]], 
                    [[1, 0, "127.0.0.1:12340"]], 
                    [[3, 1, "127.0.0.1:12341"]], 
                    [[6, 3, "127.0.0.1:12343"]]],
                   @my_nodes.map{|n| n.me.to_a})   
      puts "done"
                   
      # check localkeys
      print "checking localkeys"
      # xqout = xq.pop
      print "."
      # puts "got #{xqout.inspect}"
      @my_nodes.each { |n| n.sync_do }
      assert_equal([["127.0.0.1:12340", []], 
                    ["127.0.0.1:12341", [[1, ""]]], 
                    ["127.0.0.1:12343", [[2, ""]]], 
                    ["127.0.0.1:12346", [[6, ""]]]],
                   @my_nodes.map{|n| [n.ip_port, n.localkeys.to_a]})
      puts "done"
    
      print "checking fingers"
      7.times{fuq.pop; print "."}
      @my_nodes.each { |n| n.sync_do }
      # check fingers
      assert_equal([[0, 7, 0, 0, "127.0.0.1:12340"], [1, 0, 2, 0, "127.0.0.1:12340"], [2, 2, 6, 3, "127.0.0.1:12343"]],
                   @my_nodes[3].finger.to_a.sort)
      assert_equal([[0, 1, 2, 1, "127.0.0.1:12341"], [1, 2, 4, 3, "127.0.0.1:12343"], [2, 4, 0, 6, "127.0.0.1:12346"]], 
                   @my_nodes[0].finger.to_a.sort)
      assert_equal([[0, 2, 3, 3, "127.0.0.1:12343"], [1, 3, 5, 3, "127.0.0.1:12343"], [2, 5, 1, 6, "127.0.0.1:12346"]],
                   @my_nodes[1].finger.to_a.sort)
      assert_equal([[0, 4, 5, 6, "127.0.0.1:12346"], [1, 5, 7, 6, "127.0.0.1:12346"], [2, 7, 3, 0, "127.0.0.1:12340"]],
                   @my_nodes[2].finger.to_a.sort)
      puts "done"
                   
      # check lookup consistency
      print "checking lookups"
      @my_nodes.each{|n| n.sync_do {n.do_lookups}}
      ha = {}
      begin  
        it = q.pop
        print "."
        ha[it[0]] ||= []
        ha[it[0]] += it[1]
        ha[it[0]].uniq!
      end while ha.size < 4 or ha.map{|k,v| v.length}.uniq != [3]
      @my_nodes.each{|n| n.stop_bg}
      assert_equal(3, @my_nodes[0].succ_cache.length)
      assert_equal(@my_nodes[0].succ_cache.map{|s| s}, @my_nodes[1].succ_cache.map{|s| s})
      assert_equal(@my_nodes[1].succ_cache.map{|s| s}, @my_nodes[2].succ_cache.map{|s| s})
      assert_equal(@my_nodes[2].succ_cache.map{|s| s}, @my_nodes[3].succ_cache.map{|s| s})    
      puts "done"
    end
end
