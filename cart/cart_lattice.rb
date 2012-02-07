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
class CartLattice < Bud::Lattice
  lattice_name :lcart

  def initialize(i={})
    # Sanity check the set of operations in the cart
    i.each do |k,v|
      op_type, op_val = v

      case op_type
      when ACTION_OP
        reject_input(i) unless (op_val.class <= Enumerable && op_val.size == 2)
      when CHECKOUT_OP
      else
        reject_input(i)
      end
    end

    checkout_ops = get_checkouts(i)
    reject_input(i) unless checkout_ops.size <= 1
    unless checkout_ops.empty?
      ubound, op_val = checkout_ops.first
      lbound = op_val.last

      # All the IDs in the cart should be between the lbound ID and the ID of
      # the checkout message (inclusive).
      i.each do |k,_|
        reject_input(i) unless (k >= lbound && k <= ubound)
      end
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

  morph :cart_done
  def cart_done
    @done = compute_done if @done.nil?
    Bud::BoolLattice.new(@done)
  end

  morph :contents
  def contents
    @done = compute_done if @done.nil?
    return Bud::SetLattice.new unless @done

    actions = @v.values.select {|v| v.first == ACTION_OP}
    item_cnt = {}
    actions.each do |a|
      op_type, op_val = a
      item_id, mult = op_val
      item_cnt[item_id] ||= 0
      item_cnt[item_id] += mult
    end

    item_ary = item_cnt.select {|_,v| v > 0}.to_a
    Bud::SetLattice.new(item_ary)
  end

  private
  def get_checkouts(i)
    i.select {|_, v| v.first == CHECKOUT_OP}
  end

  def compute_done
    c_list = get_checkouts(@v)
    return false if c_list.empty?

    ubound, op_val = c_list.first
    lbound = op_val.last
    (lbound..ubound).each do |n|
      return false unless @v.has_key? n
    end

    return true
  end
end
