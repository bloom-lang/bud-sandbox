require 'rubygems'
require 'bud'
require 'test/unit'
require 'chord_node'
require 'chord_find'

class LilChord < Bud
  include ChordNode
  include ChordFind

  def initialize(ip, port, *flags)
    @addrs = {0 =>'localhost:12340', 1 => 'localhost:12341', 3 => 'localhost:12343'}
    @maxkey = 8
    super(ip,port,*flags)
  end

  def state
    super
    table :succ_cache, succ_resp.keys, succ_resp.cols
  end

  # figure 3(b) from stoica's paper
  # interface input, :find_event, ['key', 'from']
  #   interface output, :closest, ['key'], ['index', 'start', 'hi', 'succ', 'succ_addr']
  #   table :finger, ['index'], ['start', 'hi', 'succ', 'succ_addr']
  #   table :me, [], ['start']
  #   scratch :smaller, ['key', 'index', 'start', 'hi', 'succ', 'succ_addr']
  #   table :localkeys, ['key'], ['val']

  def bootstrap
    me << [@ip_port.split(':')[1].to_i%10]
    if me.first == [0]
      finger <= [[0,1,2,1,@addrs[1]], [1,2,4,3,@addrs[3]], [2,4,0,0,@addrs[0]]]
      localkeys <= [[6, '']]
    elsif me.first == [1]
      finger <= [[0,2,3,3,@addrs[3]], [1,3,5,3,@addrs[3]], [2,5,1,0,@addrs[0]]]
      localkeys <= [[1, '']].map{|t| t if @ip_port.split(":")[1][-1..-1].to_i == 1}
    elsif me.first == [3]
      finger <= [[0,4,5,0,@addrs[0]], [1,5,7,0,@addrs[0]], [2,7,3,0,@addrs[0]]]
      localkeys <= [[2, '']]
    end
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
    @addrs = {0 => 'localhost:12340', 1 => 'localhost:12341', 3 => 'localhost:12343'}
    @nodes = @addrs.values.map do |a|
      LilChord.new(a.split(':')[0], a.split(':')[1], {'visualize' => true})
    end
    assert_nothing_raised(RuntimeError) { @nodes.each{|n| n.run_bg} }

    sleep 2
    assert_equal(3, @nodes[0].succ_cache.length)
    assert_equal(@nodes[0].succ_cache.map{|s| s}, @nodes[1].succ_cache.map{|s| s})
    assert_equal(@nodes[1].succ_cache.map{|s| s}, @nodes[2].succ_cache.map{|s| s})
    assert_equal(@nodes[2].succ_cache.map{|s| s}, @nodes[0].succ_cache.map{|s| s})
  end
end
