require 'rubygems'
require 'bud'

ACTION_OP = 0
CHECKOUT_OP = 1

# The CartLattice represents the state of an in-progress or checked-out shopping
# cart. The cart can hold two kinds of items: add/remove operations, and
# checkout operations. Both kinds of operations are identified with a unique ID;
# internally, the set of items is represented as a map from ID to value. Each
# value in the map is a pair: [op_type, op_val]. op_type is either ACTION_OP or
# CHECKOUT_OP.
#
# For ACTION_OPs, the value is a nested pair: [item_id, mult], where mult is the
# incremental change to the number of item_id's in the cart (positive or
# negative).
#
# For CHECKOUT_OPs, the value is a single number, lbound. This identifies the
# smallest ID number that must be in the cart for it to be complete; we also
# assume that carts are intended to be "dense" -- that is, that a complete cart
# includes exactly the operations with IDs from lbound to the CHECKOUT_OP's
# ID. Naturally, a given cart can only have a single CHECKOUT_OP.
#
# If a cart contains "illegal" messages (those with IDs before the lbound or
# after the checkout message's ID), we raise an error. We could instead
# ignore/drop such messages; this would still yield a convergent result. We also
# raise an error if multiple checkout messages are merged into a single cart;
# this is naturally a non-confluent situation, so we need to raise an error.
#
# Why bother with a custom lattice to represent the cart state? The point is
# that checkout becomes a monotonic operation, because each replica of the cart
# can decide when it is "sealed" independently (and consistently!).
class CartLattice < Bud::Lattice
  lattice_name :lcart

  def initialize(i={})
    # Sanity check the set of operations in the cart
    i.each do |k,v|
      op_type, op_val = v

      reject_input(i) unless [ACTION_OP, CHECKOUT_OP].include? op_type
      if op_type == ACTION_OP
        reject_input(i) unless (op_val.class <= Enumerable && op_val.size == 2)
      end
    end

    checkout = get_checkout(i)
    if checkout
      ubound, _, lbound = checkout.flatten

      # All the IDs in the cart should be between the lbound ID and the ID of
      # the checkout message (inclusive).
      i.each {|k,_| reject_input(i) unless (k >= lbound && k <= ubound) }
    end

    @v = i
  end

  def merge(i)
    rv = @v.merge(i.reveal) do |k, lhs_v, rhs_v|
      raise Bud::Error unless lhs_v == rhs_v
      lhs_v
    end
    return CartLattice.new(rv)
  end

  morph :sealed
  def sealed
    @sealed = compute_sealed if @sealed.nil?
    Bud::BoolLattice.new(@sealed)
  end

  morph :summary
  def summary
    @sealed = compute_sealed if @sealed.nil?
    return Bud::SetLattice.new unless @sealed

    actions = @v.values.select {|v| v.first == ACTION_OP}
    summary = {}
    actions.each do |a|
      _, item_id, mult = a.flatten
      summary[item_id] ||= 0
      summary[item_id] += mult
    end

    # Drop deleted cart items and convert to array of pairs
    Bud::SetLattice.new(summary.select {|_,v| v > 0}.to_a)
  end

  private
  def get_checkout(i)
    lst = i.select {|_, v| v.first == CHECKOUT_OP}
    reject_input(i) unless lst.size <= 1
    lst.first   # Return checkout action or nil
  end

  def compute_sealed
    checkout = get_checkout(@v)
    return false unless checkout

    ubound, _, lbound = checkout.flatten
    (lbound..ubound).each do |n|
      return false unless @v.has_key? n
    end

    return true
  end
end
