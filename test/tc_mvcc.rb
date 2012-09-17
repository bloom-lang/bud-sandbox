require "./test_common"
require "kvs/kvs"
require "kvs/mvcc"

class TestMVCC < MiniTest::Unit::TestCase
  class BasicMVCCTest
    include Bud
    include BasicMVCC
  end

  def test_happy_path
    mvcc = BasicMVCCTest.new
    mvcc.run_bg
    transaction_time = mvcc.sync_callback :new_transaction, [[:Me]], :transaction_id_response
    mvcc.kvput <+ [[:Me, :key, transaction_time[0][0], "value"]]
    mvcc.sync_do {}
    get_response = mvcc.sync_callback :kvget, [[transaction_time[0][0], :key]], :kvget_response
    mvcc.stop
  end

  def test_garbage_collection
    mvcc = BasicMVCCTest.new
    mvcc.run_bg
    transaction_oldest = mvcc.sync_callback :new_transaction, [[:oldest]], :transaction_id_response
    mvcc.kvput <+ [[:older, :key, transaction_oldest[0][0], "value1"]]
    mvcc.sync_do { }
    mvcc.commit <+ [[transaction_oldest[0][0], :older]]
    mvcc.sync_do { }
    transaction_older = mvcc.sync_callback :new_transaction, [[:older]], :transaction_id_response
    mvcc.kvput <+ [[:older, :key, transaction_older[0][0], "value1"]]
    mvcc.sync_do { }
    mvcc.commit <+ [[transaction_older[0][0], :older]]
    mvcc.sync_do { }
    transaction_old = mvcc.sync_callback :new_transaction, [[:old]], :transaction_id_response
    mvcc.kvput <+ [[:old, :key, transaction_old[0][0], "value2"]]
    mvcc.sync_do { }
    mvcc.commit <+ [[transaction_old[0][0], :old]]
    mvcc.sync_do { }
    mvcc.transaction_id_lookup.to_a.each do |saved_id| 
      assert saved_id.transaction_id != transaction_oldest[0][0]
    end
    mvcc.stop
  end

  def test_grants_all_transaction_requests
    mvcc = BasicMVCCTest.new
    mvcc.run_bg
    done = false;
    mvcc.register_callback :transaction_id_response do |transaction_id_responses|
      transaction_id_responses.to_a.each do |transaction_id_response|
        done = true if transaction_id_response.to_a[0] >=2
      end
    end
    mvcc.new_transaction <+ [[:Me], [:You], [:EveryoneWeKnow]]
    3.times { mvcc.sync_do {} }
    while not done
      sleep 100
    end
    mvcc.stop
  end

  def test_abort_me
    mvcc = BasicMVCCTest.new
    mvcc.run_bg
    you = mvcc.sync_callback :new_transaction, [[:you]], :transaction_id_response
    me = mvcc.sync_callback :new_transaction, [[:me]], :transaction_id_response
    mvcc.kvput <+ [[:me, :key, me[0][0], "value"]]
    mvcc.sync_do {}
    aborted = mvcc.sync_callback :kvput, [[:you, :key, you[0][0], "newvalue"]], :aborted_transactions
    assert_equal me, aborted
    mvcc.stop
  end

  def test_abort_you
    mvcc = BasicMVCCTest.new
    mvcc.run_bg
    me = mvcc.sync_callback :new_transaction, [[:me]], :transaction_id_response
    you = mvcc.sync_callback :new_transaction, [[:you]], :transaction_id_response
    mvcc.kvput <+ [[:me, :key, me[0][0], "value"]]
    mvcc.sync_do {}
    aborted = mvcc.sync_callback :kvput, [[:you, :key, you[0][0], "newvalue"]], :aborted_transactions
    assert_equal you, aborted
    mvcc.stop
  end
  
  def test_get_old_value
    mvcc = BasicMVCCTest.new
    mvcc.run_bg
    transaction_old = mvcc.sync_callback :new_transaction, [[:old]], :transaction_id_response

    mvcc.kvput <+ [[:old, :key, transaction_old[0][0], "value"]]
    mvcc.sync_do { }
    mvcc.commit <+ [[transaction_old[0][0], :old]]
    mvcc.sync_do { }
    transaction_new = mvcc.sync_callback :new_transaction, [[:new]], :transaction_id_response
    resp = mvcc.sync_callback :kvget, [[transaction_new[0][0], :key]], :kvget_response
    assert_equal "value", resp[0][2]
    mvcc.stop
  end

  def test_uncommitted_snapshot_unnaffected_by_commit
    mvcc = BasicMVCCTest.new
    mvcc.run_bg
    transaction_old = mvcc.sync_callback :new_transaction, [[:old]], :transaction_id_response
    mvcc.kvput <+ [[:old, :key, transaction_old[0][0], "old_value"]]
    mvcc.sync_do { }
    mvcc.commit <+ [[transaction_old[0][0], :old]]
    mvcc.sync_do { }
    t1 = mvcc.sync_callback :new_transaction, [[:t1]], :transaction_id_response
    mvcc.kvput <+ [[:t1, :key, t1[0][0], "new_value"]]
    mvcc.sync_do { }
    t2 = mvcc.sync_callback :new_transaction, [[:t2]], :transaction_id_response
    mvcc.sync_do { }
    mvcc.sync_do { }
    mvcc.commit <+ [[:t1, t1[0][0]]]
    mvcc.sync_do { }
    resp = mvcc.sync_callback :kvget, [[t2[0][0], :key]], :kvget_response
    assert_equal "old_value", resp[0][2]
    mvcc.stop
  end

  def test_aborted_data_gone
    mvcc = BasicMVCCTest.new
    mvcc.run_bg
    you = mvcc.sync_callback :new_transaction, [[:you]], :transaction_id_response
    me = mvcc.sync_callback :new_transaction, [[:me]], :transaction_id_response
    mvcc.kvput <+ [[:me, :key, me[0][0], "value"]]
    mvcc.sync_do {}
    aborted = mvcc.sync_callback :kvput, [[:you, :key, you[0][0], "newvalue"]], :aborted_transactions
    mvcc.commit <+ [[you[0][0], :you]]
    mvcc.sync_do {}
    mvcc.sync_do {}
    sentinel = mvcc.sync_callback :new_transaction, [[:sentinel]], :transaction_id_response
    resp = mvcc.sync_callback :kvget, [[sentinel[0][0], :key]], :kvget_response
    assert_equal "newvalue", resp[0][2]
    mvcc.stop
  end

  def test_quick_write_commit_no_clobber
    mvcc = BasicMVCCTest.new
    mvcc.run_bg
    tortoise = mvcc.sync_callback :new_transaction, [[:tortoise]], :transaction_id_response
    mvcc.sync_do {}
    hare = mvcc.sync_callback :new_transaction, [[:hare]], :transaction_id_response
    mvcc.kvput <+ [[:hare, :key, hare[0][0], "hare was here"]]
    mvcc.sync_do {}
    mvcc.sync_do {}
    mvcc.commit <+ [[hare[0][0], :hare]]
    mvcc.sync_do {}
    mvcc.sync_do {}
    mvcc.kvput <+ [[:tortoise, :key, tortoise[0][0], "tortoise was here"]]
    mvcc.sync_do {}
    mvcc.sync_do {}
    mvcc.commit <+ [[tortoise[0][0], :tortoise]]
    mvcc.sync_do {}
    mvcc.sync_do {}
    sentinel = mvcc.sync_callback :new_transaction, [[:sentinel]], :transaction_id_response
    resp = mvcc.sync_callback :kvget, [[sentinel[0][0], :key]], :kvget_response
    assert_equal "hare was here", resp[0][2]
    mvcc.stop
  end
end
