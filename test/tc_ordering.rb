require 'rubygems'
require 'test/unit'
require 'bud'
require 'ordering/serializer'
require 'ordering/nonce'
require 'ordering/assigner'

class ST
  include Bud
  include Serializer

  state do
    table :mems, [:reqid, :ident, :payload]
  end

  bloom do
    mems <= dequeue_resp
  end
end

class SN
  include Bud
  include TimestepNonce
end

class GN
  include Bud
  include GroupNonce
  include StaticMembership
end

class TestSer < Test::Unit::TestCase
  def Ntest_group_nonce
    gn = GN.new
    gn.my_id <+ [[1]]
    gn.add_member <+ [[1, 'foo']]
    gn.add_member <+ [[2, 'bar']]
    gn.add_member <+ [[3, 'baz']]
    gn.seed <+ [[nil]]

    gn.run_bg

    (0..30).each do |t|
      rem = nil
      gn.sync_do { rem = gn.nonce.first }
      assert_equal(t * 3 + 1, rem[0])
    end
    gn.stop
  end

  def test_simple_nonce
    sn = SN.new(:dump => true, :port => 235235)
    sn.run_bg

    old_val = 0
    (0..10).each do |i|
      sn.sync_do {
        val = sn.nonce.first.ident
        assert(val != old_val, "nonce had same value: #{old_val} (on run #{i})")
        old_val = val
      }
      # doesn't work.
      #sn.localtick <~ [['1234']]
    end
    sn.stop
  end

  def test_serialization
    st = ST.new
    st.run_bg

    st.async_do { st.enqueue <+ [[1, 'foo']] }
    st.async_do { st.enqueue <+ [[2, 'bar']] }
    s.async_do { st.enqueue <+ [[3, 'baz']] }
    st.sync_do
    st.sync_do {
      st.dequeue <+ [[1234]]
    }
    st.sync_do
    st.sync_do {
      assert_equal(1, st.mems.length)
      assert_equal([1234, 1, 'foo'], st.mems.first)
    }

    st.sync_do { st.dequeue <+ [[2345]] }
    st.sync_do
    st.sync_do {
      assert_equal(2, st.mems.length)
      st.mems.each_with_index do |m, i|
        case i
          when 0 then assert_equal([1234, 1, 'foo'], m)
          when 1 then assert_equal([2345, 2, 'bar'], m)
        end
      end
    }
    st.stop
  end
end

class TestAssign < Test::Unit::TestCase
  class BasicSortAssign
    include Bud
    include SortAssign
  end

  def test_sort_assign
    b = BasicSortAssign.new
    b.run_bg
    v = (1..100).to_a.map {|a| [a]}
    golden = (0..99).to_a.zip(v)
    25.times do
      r = b.sync_callback(:dump, v.shuffle, :pickup)
      assert_equal(golden, r.to_a.sort)
    end
    b.stop
  end

  class BasicSortAssignP
    include Bud
    include SortAssignPersist
  end

  def test_sort_assign_persist
    b = BasicSortAssignP.new
    b.run_bg
    v = (1..100).to_a.map {|a| [a]}
    10.times do |i|
      r = b.sync_callback(:dump, v.shuffle, :pickup)
      start = i * 100
      fin = start + 99
      golden = (start..fin).to_a.zip(v)
      assert_equal(golden, r.to_a.sort)
    end
    b.stop
  end
end
