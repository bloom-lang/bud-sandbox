require 'rubygems'
require 'bud'
require 'backports'

require 'membership/membership'
require 'delivery/delivery'
require 'delivery/multicast'
require 'ordering/sequences'
require 'util/colour'

# @abstract LeaderMembership is the module for Paxos leader election.
# A given node in Paxos should include this module.
module LeaderMembership
  include MembershipProtocol
  include MulticastProtocol
  include SequencesProtocol
  include DeliveryProtocol
  include Colour

  # Each node believes it is its own leader when it first starts up.
  bootstrap do
    me <= [[ip_port]]
    new_leader <= [[ip_port]]
    add_member <= [[ip_port, ip_port]]
    get_count <= [[:mcast_msg]]
  end

  state do
    # Currently known leader
    table :leader, [] => [:host]
    # My own address
    table :me, [] => [:host]

    # Scratches for potential new leaders
    scratch :new_leader, [] => [:host]
    scratch :potential_new_leader, [:host]
    scratch :temp_new_leader, leader.schema

    # Scratches for potential new members
    scratch :potential_member, [:host]

    # Scratches to maintain if we received a leader vote message or
    # a list of members
    scratch :leader_vote, [:src, :host]
    scratch :member_list, [:src, :members]

    scratch :members_to_send, [:host]
    scratch :should_increment_mcast_count, [:key]
  end

  bloom :debug do
    magenta <= leader { |l| ["leader: #{l.host}"] }
    blue <= leader_vote { |lv| ["leader_vote: #{lv.inspect}"] }
    green <= member_list { |ml| ["member_list: #{ml.inspect}"] }
    red <= pipe_out { |po| ["pipe_out: #{po.inspect}"] }
  end

  # Each node, when receiving a message from pipe_out, needs to determine
  # the type of message. Messages are determined by the following: the
  # payload looks like [:vote, :host] or [:members, [:mem1, :mem2, ...]]
  # Because of the different types of messages, we need to demultiplex
  # the messages into the appropriate scratches.
  bloom :demux do
    leader_vote <= pipe_out do |p|
      if p.payload[0] == "vote"
        [p.src, p.payload[1]]
      end
    end
    member_list <= pipe_out do |p|
      if p.payload[0] == "members"
        [p.src, p.payload[1]]
      end
    end
  end

  # From leader_vote messages, add the source and the host to a scratch of
  # potential members. From member_list messages, add them to potential
  # members. Those who are not in the member list should be added to the
  # membership.
  bloom :add_member do
    potential_member <= leader_vote { |u| [u.src] }
    potential_member <= leader_vote { |u| [u.host] }
    potential_member <= member_list.flat_map do |ml|
      ml.members.each.map { |m| [m] }
    end
    add_member <= potential_member { |n| [n.host, n.host] }
  end

  # Changes the leader under one of two conditions:
  # 1. I get a leader_vote proposing a leader with a lower host
  # 2. Another node has joined without notifying me and its host is
  # lowest in my list of members.
  bloom :change_leader do
    potential_new_leader <= (leader_vote * leader).pairs do |lv, l|
      if lv.host < l.host
        [lv.host]
      end
    end
    temp_new_leader <= member.group([], min(:host))
    potential_new_leader <= temp_new_leader.notin(leader, :host => :host)
    new_leader <= potential_new_leader.group([], min(:host))
    leader <+- new_leader
  end

  # If there is a new leader, multicast the message to everyone in my list
  # of members.
  # If the one who told me of a "possible" new leader is wrong, send the
  # correct leader back to that source.
  bloom :notify do
    get_count <= [[:mcast_msg]]
    temp :did_add_member <= added_member.group([], count(:ident))
    mcast_send <= (return_count *
                   new_leader).pairs do |r, n|
      if r.ident == :mcast_msg
        ["vote_#{r.tally}", [:vote, n.host]]
      end
    end

    increment_count <= leader_vote { |lv| [[:unicast, lv.host]] }
    get_count <= leader_vote { |lv| [[:unicast, lv.host]] }
    pipe_in <= (return_count * leader_vote * leader).combos do |r, lv, l|
      if lv.host > l.host and r.ident == [:unicast, lv.host]
        [lv.src, ip_port, "vote_#{r.tally}", [:vote, l.host]]
      end
    end
  end

  bloom :increment_mcast_count do
    should_increment_mcast_count <= temp_new_leader { |n| [:mcast_msg] }
    should_increment_mcast_count <= did_add_member { |n| [:mcast_msg] }
    increment_count <= should_increment_mcast_count
  end

  # I send to my members my list of memers if I added someone new and
  # I am the leader
  bloom :leader_specific do
    members_to_send <= member { |m| [m.host] }
    mcast_send <= (return_count *
                   did_add_member *
                   leader * me).combos(leader.host =>
                                       me.host) do |r, d, l, m|
      if r.ident == :mcast_msg
        ["member_#{r.tally}", [:members, members_to_send.map { |ms| ms }.flatten]]
      end
    end
  end

end
