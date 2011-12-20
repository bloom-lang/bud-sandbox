require 'delivery/reliable'
require 'membership/membership.rb'

# @abstract MulticastProtocol is the abstract interface for multicast message delivery.
# A multicast implementation should subclass MulticastProtocol and mix in a
# chosen DeliveryProtocol and MembershipProtocol.
module MulticastProtocol
  state do
    # Used to request that a new message be delivered to all members in the
    # Membership module.
    # @param [Number] ident a unique id for the message
    # @param [Object] payload the message payload
    interface input, :mcast_send, [:ident] => [:payload]

    # Used to indicate that a new message has been delivered to all members.
    # @param [Number] ident the unique id of the delivered message
    interface output, :mcast_done, [:ident]
  end
end

# @abstract Multicast is an abstract implementation for multicast message delivery. The functionality is implemented, but it depends on an implemented DeliveryProtocol and MembershipProtocol to be mixed in.
# A simple implementation of Multicast, which depends on abstract delivery
# and membership modules.
module Multicast
  include MulticastProtocol
  include DeliveryProtocol
  include MembershipProtocol

  state do
    # Keeps track of the number of messages that need to be confirmed
    # as sent for a given mcast id.
    table :unacked_count, [:ident] => [:num]
    # A scratch noting the number of members in this tick.
    scratch :num_members, [] => [:num]
    # A scratch of the number of messages confirmed this timestep for
    # a given mcast id.
    scratch :acked_count, [:ident] => [:num]
  end
  
  bloom :snd_mcast do 
    pipe_in <= (mcast_send * member).pairs do |s, m|
      [m.host, ip_port, s.ident, s.payload] unless m.host == ip_port
    end
  end

  bloom :init_unacked do
    num_members <= member.group(nil, count)
    unacked_count <= (mcast_send * num_members).pairs do |s, c|
      [s.ident, c.num]
    end
  end

  bloom :done_mcast do
    acked_count <= pipe_sent.group([:ident], count(:ident))
    unacked_count <+- (acked_count * unacked_count).pairs do |a, u|
      [a.ident, u.num - a.num]
    end
    mcast_done <= unacked_count {|u| [u.ident] if u.num == 0}
    unacked_count <- unacked_count {|u| u if u.num == 0}
  end
end

module BestEffortMulticast
  include BestEffortDelivery
  include Multicast
  include StaticMembership
end

module ReliableMulticast
  include ReliableDelivery
  include Multicast
  include StaticMembership
end
