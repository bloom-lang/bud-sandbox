require 'rubygems'
require 'bud'
require 'test/unit'
require 'chord/chord_node'
require 'chord/chord_find'
require 'chord/chord_join'

class LilChord
  include Bud
  include ChordNode
  include ChordFind
  include ChordJoin

  def initialize(opts)
    @addrs = {0 =>'localhost:12340', 1 => 'localhost:12341', 3 => 'localhost:12343'}
    @maxkey = 8
    super
  end

  state {
    table :succ_cache, succ_resp.key_cols => succ_resp.cols
  }

  # figure 3(b) from stoica's paper
  # interface input, :find_event, ['key', 'from']
  #   interface output, :closest, ['key'], ['index', 'start', 'hi', 'succ', 'succ_addr']
  #   table :finger, ['index'], ['start', 'hi', 'succ', 'succ_addr']
  #   table :me, [], ['start']
  #   scratch :smaller, ['key', 'index', 'start', 'hi', 'succ', 'succ_addr']
  #   table :localkeys, ['key'], ['val']

  def load_data
    me << [@options[:port].to_i%10]
    if me.first == [0]
      finger <= [[0,1,2,1,@addrs[1]], [1,2,4,3,@addrs[3]], [2,4,0,0,@addrs[0]]]
      localkeys <= [[6, '']]
    elsif me.first == [1]
      finger <= [[0,2,3,3,@addrs[3]], [1,3,5,3,@addrs[3]], [2,5,1,0,@addrs[0]]]
      localkeys <= [[1, '']]
    elsif me.first == [3]
      finger <= [[0,4,5,0,@addrs[0]], [1,5,7,0,@addrs[0]], [2,7,3,0,@addrs[0]]]
      localkeys <= [[2, '']]
    end
  end
  
  def do_lookups
    # issue local lookups for 1,2,6
    [1,2,6].each do |num|
      succ_req <+ [[num]]
    end
  end

  declare
  def persist_resps
    succ_cache <= succ_resp
    # stdio <~ succ_resp.map{|s| [s.inspect]}
  end
end

class TestFind < Test::Unit::TestCase
  def test_find
    @addrs = {0 => 12340, 1 => 12341, 3 => 12343}
    @my_nodes = @addrs.values.map do |a|
      LilChord.new(:port => a) #, :visualize => 3)
    end
    assert_nothing_raised { @my_nodes.each{|n| n.run_bg} }
    assert_nothing_raised { @my_nodes.each{|n| n.sync_do {n.load_data}}}
    assert_nothing_raised { @my_nodes.each{|n| n.sync_do {n.do_lookups}}}
    sleep 2
    assert_nothing_raised { @my_nodes.each{|n| n.stop_bg} }
    
    # [0,1,2].each do |num|
    #   puts "node #{@my_nodes[num].port} : #{@my_nodes[num].succ_cache.map.inspect}"
    # end
    assert_equal(3, @my_nodes[0].succ_cache.length)
    assert_equal(@my_nodes[0].succ_cache.map{|s| s}, @my_nodes[1].succ_cache.map{|s| s})
    assert_equal(@my_nodes[1].succ_cache.map{|s| s}, @my_nodes[2].succ_cache.map{|s| s})
  end
  # def test_join
  #   @addrs = {0 => 12340, 1 => 12341, 3 => 12343}
  #   @my_nodes = @addrs.values.map do |a|
  #     LilChord.new(:port => a, :visualize => 3)
  #   end
  #   assert_nothing_raised { @my_nodes.each{|n| n.run_bg} }
  #   assert_nothing_raised { @my_nodes.each{|n| n.async_do {n.load_data}}}
  #   @addrs[6] = 12346
  #   newnode = LilChord.new(:port => 12346, :visualize => 3)
  #   @my_nodes << newnode
  #   assert_nothing_raised { newnode.run_bg}
  #   assert_nothing_raised { newnode.async_do{newnode.join_req <~ [['localhost:12340', 'localhost:12346', 6]]}}
  #   sleep 2
  #   assert_nothing_raised { @my_nodes.each{|n| n.sync_do {n.do_lookups}}}
  #   sleep 2
  #   assert_nothing_raised { @my_nodes.each{|n| n.stop_bg} }
  #   # assert_equal(4, @my_nodes[0].succ_cache.length)
  #   assert_equal(@my_nodes[0].finger.map{|f| f}, nil)
  #   assert_equal(@my_nodes[0].succ_cache.map{|s| s}, @my_nodes[1].succ_cache.map{|s| s})
  #   assert_equal(@my_nodes[1].succ_cache.map{|s| s}, @my_nodes[2].succ_cache.map{|s| s})
  #   assert_equal(@my_nodes[2].succ_cache.map{|s| s}, @my_nodes[3].succ_cache.map{|s| s})
  # end
end
