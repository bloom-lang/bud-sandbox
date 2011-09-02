require 'test/unit'
require 'ordering/vector_clock'

class TestVectorClock < Test::Unit::TestCase
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
end
