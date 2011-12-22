# Written by Max Johnson and Ryan Spore

require 'rubygems'
require 'bud'
require 'kvs/kvs'
require 'ordering/queue'
require 'ordering/sequences'

# NOTES:
#   * We disallow writes from clients to transactions they don't own. Ideally, we would also enforce this for get and del, but the KVSProtocol doesn't support it.


# @abstract MVCCProtocol is an abstract interface to an MVCC-based key-value store.
# an MVCC implementation should subclass MVCCProtocol
module MVCCProtocol
  include KVSProtocol
  state do
    
    # used to request a new transaction id and start said transaction
    # @param [String] client a unique client id, e.g. an ip address
    interface input, :new_transaction, [:client] => []
    
    # returns a new transaction id to the client who requested it
    # @param [Number] transaction_id a unique transaction id to be used for read and write requests
    # @param [String] client a unique client id, e.g. an ip address
    interface output, :transaction_id_response, [:transaction_id] => [:client]
    
    # used to commit a completed transaction
    # @param [Number] transaction_id a unique transaction id to be used for read and write requests; the transaction to be commited
    # @param [String] client a unique client id, e.g. an ip address
    interface input, :commit, [:transaction_id] => [:client]
    
    # a tuple in this output indicates that a transaction has been aborted
    # @param [Number] transaction_id a unique transaction id to be used for read and write requests; the transaction that was aborted
    # @param [String] client a unique client id, e.g. an ip address
    interface output, :aborted_transactions, [:transaction_id] => [:client]
  end
end

module MVCC
  include MVCCProtocol
  import KVSProtocol => :kvs
  include Counter
  include FIFOQueue
  
  state do
    table :active_transactions, [:transaction_id] => [:client]
    scratch :new_active_transaction, active_transactions.schema
    scratch :signal_commit, [:transaction_id]
    scratch :signal_abort, [:transaction_id] => [:client]
    scratch :write_request, kvput.schema
    scratch :perform_write, write_request.schema
    table :write_log, [:transaction_id, :key] => [:client]
    table :transaction_id_lookup, [:key, :transaction_id] => []
    scratch :old_transactions, transaction_id_lookup.schema
    scratch :older_transactions, transaction_id_lookup.schema
    scratch :oldest_transaction, active_transactions.schema
    scratch :garbage_collection_keys, [:key] => [:transaction_id]
    table :snapshot_lookup, [:transaction_id, :key] => [:lookup_id]
  end

  bloom :new_transactions do
    push <= new_transaction { |t| [t, 0] }
    pop <= [[0]]
    get_count <= [[:next_transaction_id]]
    new_active_transaction <= (return_count * pop_response).pairs do |count, t|
      [count.tally, t.item.client]
    end
    increment_count <= new_active_transaction { |t| [:next_transaction_id] }
    active_transactions <= new_active_transaction
    older_transactions <= (new_active_transaction * transaction_id_lookup).pairs() do |new_transaction, writes_for_key|
      [writes_for_key.key, writes_for_key.transaction_id] if writes_for_key.transaction_id <= new_transaction.transaction_id
    end
    snapshot_lookup <= (new_active_transaction * older_transactions.argmax([:key], :transaction_id)).pairs {|new_tx, key| [new_tx.transaction_id, key.key, key.transaction_id]}
    transaction_id_response <= new_active_transaction
  end
  
  bloom :finish_transaction_successfully do
    signal_commit <= (commit * active_transactions).lefts(:client => :client)
    active_transactions <- (active_transactions * signal_commit).lefts(:transaction_id => :transaction_id)
    write_log <- (write_log * signal_commit).lefts(:transaction_id => :transaction_id)
    transaction_id_lookup <+ (write_log * signal_commit).lefts(:transaction_id => :transaction_id) do |w|
      [w.key, w.transaction_id]
    end
    snapshot_lookup <- (signal_commit * snapshot_lookup).rights(:transaction_id => :transaction_id)
  end
  
  bloom :kvs_put do
    write_request <= (kvput * active_transactions).lefts(:client => :client, :reqid => :transaction_id)
    signal_abort <= (write_request * write_log).pairs(:key => :key) do |this, that|
      if that.transaction_id < this.reqid
        [this.reqid, this.client]
      else
        [that.transaction_id, that.client]
      end
    end
    perform_write <= write_request.notin(signal_abort, :reqid => :transaction_id)
    write_log <+ perform_write { |w| [w.reqid, w.key, w.client] }
    kvs.kvput <= perform_write { |w| [w.client, [w.key, w.reqid], w.reqid, w.value]}
    snapshot_lookup <+- perform_write { |w| [w.reqid, w.key, w.reqid] }
  end

  bloom :kvs_get do
    kvs.kvget <= (kvget * snapshot_lookup).pairs(:key => :key, :reqid => :transaction_id) do |get_request, snapshot|
      [get_request.reqid, [get_request.key, snapshot.lookup_id]]
    end
    kvget_response <= kvs.kvget_response do |get_response|
      [get_response.reqid, get_response.key[0], get_response.value]
    end
  end
  
  bloom :kvs_del do
    kvput <= (kvdel * active_transactions).pairs(:reqid => :transaction_id) {|delete_request, active_transaction| [active_transaction.client, delete_request.key, active_transaction.transaction_id, nil]}
  end

  bloom :abort do
    active_transactions <- (active_transactions * signal_abort).lefts(:transaction_id => :transaction_id)
    write_log <- (write_log * signal_abort).lefts(:transaction_id => :transaction_id)
    transaction_id_lookup <- (write_log * signal_abort).lefts(:transaction_id => :transaction_id) do |w|
        [w.key, w.transaction_id]
    end
    snapshot_lookup <- (signal_abort * snapshot_lookup).rights(:transaction_id => :transaction_id)
    kvs.kvdel <= (write_log * signal_abort).lefts(:transaction_id => :transaction_id) { |aborted_write| [aborted_write.key, aborted_write.transaction_id] }
    aborted_transactions <= signal_abort
  end

  bloom :garbage_collection do
    oldest_transaction <= (active_transactions.argmin([], :transaction_id) * signal_commit).lefts(:transaction_id => :transaction_id)
    garbage_collection_keys <= (oldest_transaction * write_log).pairs(:transaction_id => :transaction_id) {|old, write| [write.key, old.transaction_id]}
    old_transactions <= (garbage_collection_keys * transaction_id_lookup).pairs(:key=>:key) do |key_to_check, writes_for_key|
      [writes_for_key.key, writes_for_key.transaction_id] if writes_for_key.transaction_id <= key_to_check.transaction_id
    end
    temp :x <= old_transactions.argmax([:key], :transaction_id)
    temp :y <= old_transactions.notin(x)
    transaction_id_lookup <- (y * oldest_transaction).lefts
    kvs.kvdel <= (y * oldest_transaction).lefts
  end
end

module BasicMVCC
  include MVCC
  import BasicKVS => :kvs
end

module ReplicatedMVCC
  include MVCC
  import ReplicatedKVS => :kvs
end
