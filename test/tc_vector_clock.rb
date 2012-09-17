require './test_common'
require 'ordering/vector_clock'

class TestVectorClock < MiniTest::Unit::TestCase
  def test_single_vector
    v1 = VectorClock.new

    v1.increment("C1")
    assert_equal(1, v1["C1"])
    v1.increment("C1")
    assert_equal(2, v1["C1"])
    v1.increment("C2")
    assert_equal(1, v1["C2"])
  end

  def test_vector_merge
    v1 = VectorClock.new
    v2 = VectorClock.new

    3.times { v1.increment("C1") }
    4.times { v2.increment("C1") }
    5.times { v2.increment("C2") }
    6.times { v1.increment("C2") }

    v1.merge(v2)

    assert_equal(4, v2["C1"])
    assert_equal(5, v2["C2"])

    assert_equal(4, v1["C1"])
    assert_equal(6, v1["C2"])
  end

  def test_vector_happens_before
    v1 = VectorClock.new
    v2 = VectorClock.new

    3.times { v1.increment("C1") }
    4.times { v2.increment("C1") }
    5.times { v1.increment("C2") }
    6.times { v2.increment("C2") }

    assert(v1.happens_before(v2))
    assert(!v2.happens_before(v1))

    v3 = VectorClock.new
    7.times { v3.increment("C2") }

    assert(!(v2.happens_before(v3)))

    8.times { v3.increment("C1") }
    assert(v2.happens_before(v3))

    #make sure equal vectors don't happen before
    v4 = VectorClock.new
    v5 = VectorClock.new

    v4.increment("C1")
    v5.increment("C1")

    assert(!v4.happens_before(v5))
  end

  def test_vector_happens_before_non_strict
    v1 = VectorClock.new
    v2 = VectorClock.new

    assert(v1.happens_before_non_strict(v2))
    assert(v2.happens_before_non_strict(v1))

    1.times { v2.increment("C1") }

    assert(v1.happens_before_non_strict(v2))

    1.times { v1.increment("C1") }

    assert(v1.happens_before_non_strict(v2))
    assert(v2.happens_before_non_strict(v1))

    1.times { v1.increment("C1") }

    assert(!v1.happens_before_non_strict(v2))
    assert(v2.happens_before_non_strict(v1))
  end
end
