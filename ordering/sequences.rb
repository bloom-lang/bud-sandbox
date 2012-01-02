require 'rubygems'
require 'bud'

# @abstract SequencesProtocol is the abstract interface for updating and accessing a collection of counters.
# A module that requires counters should subclass SequencesProtocol.
module SequencesProtocol
  state do
    # increment the counter for an id. If the id does not exist, initialize it.
    # @param [String] ident the unique identifier of a row in a collection of counts
    interface input, :increment_count, [:ident]

    # reset the counter for an id. In any implementation, the row identified by ident should either be set to
    # its initial value again, or no longer be in the collection of counters.
    # @param [String] ident the unique identifier of a row in a collection of counts
    interface input, :clear_ident, [:ident]

    # request the count of an id. If the id is non-existant, then get_count will initialize the given id.
    # @param [String] ident the unique identifier of a row in a collection of counts
    interface input, :get_count, [:ident]

    # output the count of an id. A get_count invocation should never result in return_count being empty.
    # @param [String] ident the unique identifier of a row in a collection of counts
    # @param [Number] the count associated with the ident
    interface output, :return_count, [:ident]=>[:tally]
  end
end

# Counter is a simple implementation of SequencesProtocol
# @see SequencesProtocol implements SequencesProtocol
module Counter
  include SequencesProtocol

  state do
    # used to keep state for all counters in Counter
    table :total_counts, [:ident] => [:tally]
  end

  bloom do
    # when first count for an ident comes in, set up new count tuple for ident
    total_counts <+ increment_count do |u|
      [u.ident, 0] if not total_counts.exists? do |t|
        u.ident==t.ident
      end
    end

    # when get count for nonexistent ident comes in, set up new count tuple for ident
    total_counts <+ get_count do |u|
      [u.ident, 0] if not total_counts.exists? do |t|
        u.ident==t.ident
      end
    end

    return_count <= get_count do |u|
      [u.ident, 0] if not total_counts.exists? do |t|
        u.ident==t.ident
      end
    end

    # increment an existing count by 1
    total_counts <+- (total_counts * increment_count).pairs(:ident=>:ident) do |l,r|
      [l.ident, l.tally+1]
    end

    # return count when get request comes in
    return_count <= (get_count*total_counts).rights(:ident=>:ident)

    # clear count when clear request comes in
    total_counts <- (clear_ident*total_counts).rights(:ident=>:ident)
  end
end
