require 'rubygems'
require 'bud'
require 'test/unit'
require 'kvs/mv_kvs'
require 'ordering/vector_clock'

class SingleSiteMVKVS
  include Bud
  include BasicMVKVS
end

class SingleSiteVCMVKVS
  include Bud
  include VC_MVKVS
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

  def test_vector_mvkvs
    v = SingleSiteVCMVKVS.new
    vc = VectorClock.new
    v.run_bg

    v.sync_do { v.kvput <+ [["testclient", "fookey", vc, "req0", "foo"]] }
    v.sync_do { v.kvput <+ [["testclient", "fookey", vc, "req1", "bar"]] }
    v.sync_do { v.kvget <+ [["req3", "fookey"]] }
    v.sync_do {
      assert_equal(2, v.kvget_response.length)
      resp = v.kvget_response.to_a.sort 
      #check that both values exist
      assert_equal(["bar", "foo"], resp.map { |r| r[3] }.sort)
      #check that vector clock values for testclient are set appropriately
      assert_equal([1,2], resp.map { |r| r[2]["testclient"] }.sort)
    }

    v.stop_bg
  end
end
