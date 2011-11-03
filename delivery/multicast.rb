require 'delivery/reliable'
require 'voting/voting'
require 'membership/membership.rb'

module MulticastProtocol
  include MembershipProtocol

  # XXX: This should provide an interface for the recipient-side to read the
  # multicast w/o needing to peek at pipe_in.
  state do
    interface input, :send_mcast, [:ident] => [:payload]
    interface output, :mcast_done, [:ident] => [:payload]
  end
end

module Multicast
  include MulticastProtocol
  include DeliveryProtocol

  bloom :snd_mcast do
    pipe_in <= (send_mcast * member).pairs do |s, m|
      [m.host, ip_port, s.ident, s.payload] unless m.host == ip_port
    end
  end

  bloom :done_mcast do
    # override me
    mcast_done <= pipe_sent {|p| [p.ident, p.payload] }
  end
end

module BestEffortMulticast
  include BestEffortDelivery
  include Multicast
end

module ReliableMulticast
  include ReliableDelivery
  include VotingMaster
  include VotingAgent
  include Multicast

  bloom :start_mcast do
    begin_vote <= send_mcast {|s| [s.ident, s] }
  end

  bloom :agency do
    cast_vote <= (pipe_sent * waiting_ballots).pairs(:ident => :ident) {|p, b| [b.ident, b.content]}
  end

  bloom :done_mcast do
    mcast_done <= victor {|v| v.content}
  end
end
