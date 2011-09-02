require 'rubygems'
require 'bud'
require 'test/unit'
require 'kvs/mv_kvs'

class SingleSiteMVKVS
  include Bud
  include BasicMVKVS
end

class TestMVKVS < Test::Unit::TestCase
  def test_simple
    v = SingleSiteMVKVS.new
    v.run_bg

    v.sync_do { v.kvput <+ [["testclient", "fookey", "v0", "req0", "foo"]] }
    v.sync_do { v.kvput <+ [["testclient", "fookey", "v1", "req1", "bar"]] }

    v.sync_do { v.kvget <+ [["req3", "fookey"]] }
    v.sync_do {
      assert_equal(2, v.kvget_response.length)
      assert_equal([["req3", "fookey", "v0", "foo"], 
                    ["req3", "fookey", "v1", "bar"]], 
                   v.kvget_response.to_a.sort)
    }

    v.stop_bg
  end
end
