require 'rubygems'
require 'bud'
require 'delivery/delivery'

# At the sender side, we assign monotonically-increasing IDs to outgoing
# messages. Since all messages sent in the same timestep are logically
# concurrent, we _could_ assign them the same ID value -- but then we'd need
# another mechanism to ensure that all tuples with a given ID value have been
# delivered.
module OrderedDelivery
  include DeliveryProtocol
end
