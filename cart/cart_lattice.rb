require 'rubygems'
require 'bud'

ACTION_OP = 0
CHECKOUT_OP = 1

# The CartLattice represents the state of an in-progress or checked-out shopping
# cart. The cart can hold two kinds of items: add/remove operations, and
# checkout operations. Both kinds of operations are identified with a unique ID;
# internally, the set of items is represented as a map from ID to value. Each
# value in the map is an array, where the first element is either ACTION_OP or
# CHECKOUT_OP.
#
# For ACTION_OPs, the rest of the array contains: item_id and mult, where mult
# is the incremental change to the number of item_id's in the cart (positive or
# negative).
#
# For CHECKOUT_OPs, the rest of the array contains lbound and # checkout_addr.
# lbound identifies the smallest ID number that must be in the cart for it to be
# complete; we also assume that carts are intended to be "dense" -- that is,
# that a complete cart includes exactly the operations with IDs from lbound to
# the CHECKOUT_OP's ID. checkout_addr is the address we want to contact with the
# completed cart state (we stash it here for convenience). Naturally, a given
# cart can only have a single CHECKOUT_OP.
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
      reject_input(i) unless (v.class <= Enumerable && v.size == 3)
      reject_input(i) unless [ACTION_OP, CHECKOUT_OP].include? v.first
    end

    checkout = get_checkout(i)
    if checkout
      ubound, _, lbound, _ = checkout.flatten

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

  morph :sealed do
    @sealed = compute_sealed if @sealed.nil?
    Bud::BoolLattice.new(@sealed)
  end

  morph :summary do
    @sealed = compute_sealed if @sealed.nil?
    raise Bud::Error unless @sealed

    actions = @v.values.select {|v| v.first == ACTION_OP}
    summary = {}
    actions.each do |a|
      _, item_id, mult = a
      summary[item_id] ||= 0
      summary[item_id] += mult
    end

    # Drop deleted cart items and return an array of pairs
    summary.select {|_,v| v > 0}.to_a.sort
  end

  morph :checkout_addr do
    checkout = get_checkout(@v)
    raise Bud::Error unless checkout
    checkout.flatten.last
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

    ubound, _, lbound, _ = checkout.flatten
    (lbound..ubound).each do |n|
      return false unless @v.has_key? n
    end

    return true
  end
end
