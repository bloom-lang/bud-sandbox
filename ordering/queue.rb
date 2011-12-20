# @abstract PriorityQueueProtocol is the abstract interface for priority queues
# Any implementation of a queue should subclass PriorityQueueProtocol
module PriorityQueueProtocol
  state do
    # Push items into the queue.
    # Useful Mnemonic: push "item" with priority "priority" into queue "queue."
    # Note: queue is essentially optional - a single queue can be used without specifying queue because it will automatically be included as nil
    # @param [Object] item is the item that will be pushed into the queue
    # @param [Number] priority specifies the priority of the item in the queue
    # @param [Number] queue specifies which queue to push the item in
    interface input, :push, [:item, :priority, :queue]

    # Removes items out of the queue, regardless of priority.
    # Useful Mnemonic: remove "item" from queue "queue"
    # @param [Object] item specifies which item to remove
    # @param [Number] queue specifies which queue to remove the item from
    # @return [remove_response] upon successful removal.
    interface input, :remove, [:item, :queue]

    # Pop items out of the queue.
    # Removes the top priority item in queue queue: outputs the item into pop_response.
    # Useful Mnemonic: pop from queue "queue"
    # @param [Number] queue specifies which queue to pop from
    # @return [pop_response] when the pop request is successfully processed.
    interface input, :pop, [:queue]

    # Peek the top item in the queue.
    # Like pop, but does not remove the item from the queue.
    # Useful Mnemonic: peek from queue "queue"
    # @param [Number] queue specifies which queue to peek at
    # @return [peek_response] when the peek request is successfully processed.
    interface input, :peek, [:queue]
    
    # If there is a remove request, remove and return the item regardless of priority
    # @param [Object] item is the item that will be pushed into the queue
    # @param [Number] priority specifies the priority of the item in the queue
    # @param [Number] queue specifies which queue to push the item in
    interface output, :remove_response, push.schema

    # If there is a pop request, remove and return the top priority item from the queue
    # @param [Object] item is the item that will be pushed into the queue
    # @param [Number] priority specifies the priority of the item in the queue
    # @param [Number] queue specifies which queue to push the item in
    interface output, :pop_response, push.schema

    # If there is a peek request, return (but don't remove) the top priority item from the queue
    # @param [Object] item is the item that will be pushed into the queue
    # @param [Number] priority specifies the priority of the item in the queue
    # @param [Number] queue specifies which queue to push the item in
    interface output, :peek_response, push.schema
  end
end

# @abstract FIFOQueueProtocol is the abstract interface for fifo queues
module FIFOQueueProtocol
  state do
    # Push items into the queue.
    # Note: queue is essentially optional - a single queue can be used without specifying queue because it will automatically be included as nil
    # @param [Object] item is the item that will be pushed into the queue
    # @param [Number] queue specifies which queue to push the item in
    interface input, :push, [:item, :queue]

    # Pop items out of the queue.
    # Removes the top priority item in queue queue: outputs the item into pop_response.
    # @param [Number] queue specifies which queue to pop from
    # @return [pop_response] when the pop request is successfully processed.
    interface input, :pop, [:queue]

    # Peek the top item in the queue.
    # Like pop, but does not remove the item from the queue.
    # @param [Number] queue specifies which queue to peek at
    # @return [peek_response] when the peek request is successfully processed.
    interface input, :peek, [:queue]

    # If there is a pop request, remove and return the first item that was inserted into the queue
    # @param [Object] item is the item that will be pushed into the queue
    # @param [Number] queue specifies which queue to push the item in
    interface output, :pop_response, [:item, :queue]

    # If there is a peek request, return (but don't remove) the first item that was inserted into the queue
    # @param [Object] item is the item that will be pushed into the queue
    # @param [Number] queue specifies which queue to push the item in
    interface output, :peek_response, [:item, :queue]
  end
end

# PriorityQueue is the basic implementation of a priority queue.
# The front of the queue is always the lowest priority item.
# @see PriorityQueue implements PriorityQueueProtocol
module PriorityQueue
  include PriorityQueueProtocol

  state do
    # The items that are currently in the queue
    table :items, [:item, :priority, :queue]

    # The lowest priority item for each queue.
    # Does not necessarily contain one item per queue (contains all items with the current lowest priority)
    scratch :lowest, [:item, :priority, :queue]

    # Temporary collection to contain the pop response.
    # Does not necessarily contain one item per queue (contains all items with the current lowest priority)
    # An interposition for breaking ties
    scratch :lowest_popr, [:item, :priority, :queue]

    # Temporary collection to contain the peek response.
    # Does not necessarily contain one item per queue (contains all items with the current lowest priority)
    # An interposition for breaking ties
    scratch :lowest_peekr, [:item, :priority, :queue]
  end

  bloom :remember do
    items <= push
  end

  bloom :calc_lowest do
    # Users can override method of choosing best priority
    # By default it is based on the ruby min
    lowest <= items.argmin([:queue], :priority)
    lowest_popr <= (pop * lowest).rights(:queue => :queue)
    lowest_peekr <= (peek * lowest).rights(:queue => :queue)
  end

  bloom :break_tie do
    # Users can override method of breaking ties
    # By default it is chosen arbitrarily
    pop_response <= lowest_popr.argagg(:choose, [:queue, :priority], :item)
    peek_response <= lowest_peekr.argagg(:choose, [:queue, :priority], :item)
  end

  bloom :remove_item do
    remove_response <= (remove * items).rights(:queue => :queue, :item => :item)
  end
  
  bloom :drop do
    items <- remove_response
    items <- pop_response
  end

  bloom :debug do
#    stdio <~ lowest.inspected
#    stdio <~ pop_response.inspected
  end
end

# FIFOQueue is the basic implementation of a fifo queue.
# The front of the queue is always the earliest item that was inserted out of the items in the queue.
# Uses budtime to order the items.
# @see FIFOQueue implements FIFOQueueProtocol
# @see FIFOQueue imports PriorityQueue
module FIFOQueue
  include FIFOQueueProtocol
  import PriorityQueue => :pq

  bloom do
    pq.push <= push {|p| [p.item, budtime, p.queue]}
    pq.pop <= pop
    pq.peek <= peek

    pop_response <= pq.pop_response {|p| [p.item, p.queue]}
    peek_response <= pq.peek_response {|p| [p.item, p.queue]}
  end
end
