require 'rubygems'
require 'bud'

require 'delivery/reliable_delivery'
require 'voting/voting'
require 'membership/membership.rb'

module MulticastProtocol
  include MembershipProto

  state do
    interface input, :send_mcast, [:ident] => [:payload]
    interface output, :mcast_done, [:ident] => [:payload]
  end
end

module Multicast
  include MulticastProtocol
  include DeliveryProtocol

  bloom :snd_mcast do
    pipe_in <= join([send_mcast, member]).map do |s, m|
      [m.host, @ip_port, s.ident, s.payload]
    end
  end

  bloom :done_mcast do
    # override me
    mcast_done <= pipe_sent.map{|p| [p.ident, p.payload] }
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
    begin_vote <= send_mcast.map{|s| [s.ident, s] }
  end

  bloom :agency do
    cast_vote <= join([pipe_sent, waiting_ballots], [pipe_sent.ident, waiting_ballots.ident]).map{|p, b| [b.ident, b.content]}
  end

  bloom :done_mcast do
    mcast_done <= vote_status.map do |v|
      "VEE: " + v.inspect
    end
  end
end
