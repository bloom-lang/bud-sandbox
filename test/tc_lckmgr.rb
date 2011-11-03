require 'rubygems'
require 'bud'
require 'test/unit'
require 'lckmgr/lckmgr'


class J1 
  include Bud
  include TwoPhaseLockMgr
end


class XactMgr
  include Bud
  include TwoPhaseLockMgr

  def tlock(xid, key)
    res = sync_callback(:request_lock, [[xid, key]], :lock_status)
    res.each do |r|
      if r.xid == xid and r.status == :OK
        return true
      end
    end
    return tlock(xid, key)
  end
  
  def endx(xid)
    sync_do { end_xact <+ [[xid]]}
    sync_do
  end
end


class TU < Test::Unit::TestCase
  ITERS = 20

  def xact_1(mg, i)
    mg.tlock(i, "foo")
    mg.tlock(i, "bar")
    mg.tlock(i, "baz")
  
    mg.tlock(i, "bimbim")
    mg.tlock(i, "fizzbuzz")
    mg.sync_do do 
      mg.lock.each do |row|
        if row[0] == "foo"
          assert_equal(i, row[1])
        end
      end
  
    end
    mg.endx i
  end


  def xact_2(mg, i)
    mg.tlock(i, "foo")
    mg.tlock(i, "bar")
    mg.tlock(i, "bam")
    mg.endx i
  end
  
  def test_concurrent
    xm = XactMgr.new
    xm.run_bg

    Thread.new do
      (0..ITERS).each do |i|
        xact_1(xm, i)
      end
    end

    (ITERS+1..ITERS+ITERS).each do |i|
      xact_2(xm, i)
    end
    
    assert(true)
    xm.stop
  end
  
  def test_two
    xm = XactMgr.new
    xm.run_bg
    xm.lock(1, "foo")
    xm.lock(1, "bar")
    xm.lock(1, "baz")

    xm.lock(2, "fuq")
    xm.lock(2, "foo")

    xm.stop

    assert(true)
  end


  def test_one
    j1 = J1.new
    j1.run_bg

    #j1.sync_do {j1.request_lock <+ [[1, "foo"]]}
    #j1.sync_do{}
    res = j1.sync_callback(:request_lock, [[1, "foo"]], :lock_status)

    res = j1.sync_callback(:request_lock, [[1, "bar"]], :lock_status)
    assert_equal([1, "bar", :OK], res.first)

    q = Queue.new
    j1.register_callback(:lock_status) do |l|
      l.each do |row|
        if row.xid == 2
          q.push row
        end
      end
    end


    j1.register_callback(:lock_status) do |l|
      l.each do |row|
        if row.xid == 3
          j1.sync_do{ j1.end_xact <+ [[3]]}
          j1.sync_do
        end
      end
    end


    j1.sync_do{ j1.request_lock <+ [[2, "foo"]]}
    j1.sync_do{ j1.request_lock <+ [[3, "foo"]]}
    j1.sync_do { assert_equal(["foo", 1], j1.lock.first) }
    j1.sync_do { j1.end_xact <+ [[1]]}
    j1.sync_do

    row = q.pop
    assert_equal([2, "foo", :OK], row)
    assert(true)
  end
end
