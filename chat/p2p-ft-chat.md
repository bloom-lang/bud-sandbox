Peer to peer, fault-tolerant chat
=========================

Writing a simple chat server in Bloom is extremely [easy](https://github.com/bloom-lang/bud/tree/master/examples/chat).
The job of a chat [client](https://github.com/bloom-lang/bud/blob/master/examples/chat/chat.rb) is to forward messages (typed into the keyboard)to a central server, and to print (to the screen) messages relayed by that server.  
The job of a [server](https://github.com/bloom-lang/bud/blob/master/examples/chat/chat_server.rb) is to maintain a list of members, and forward all messages to all members.

In this short demo, we'll evolve that toy chat server into a distributed system that is *decentralized* and *fault-tolerant*.  When we're done, we'll
have a chat program that behaves essentially the same as the original, except that 1) all nodes can play the role of client or server, and 2) when the server node fails,
one of the clients can assume its role.

Tweaks
---------

It turns out that the modifications to the original program are minimal, and fairly obvious.

    mcast <~ stdio do |s|
      [@server, [ip_port, @nick, Time.new.strftime("%I:%M.%S"), s.line]]
    end

In the original chat program, we forwarded all messages to a distinguished server running a different chunk of code.  In our p2p chat, any node could
be the server.  So we replace the reference to an instance variable with a reference to a Bloom collection:

    chatter <~ (stdio * leader).pairs do |s, l|
      [l.addr, [ip_port, @nick, Time.new, s.line]]
    end

We'll leave the declaration of *leader* and the rules that define its contents for later.

The original chat server blindly multicasted all messages it received to all clients:

    mcast <~ (mcast * nodelist).pairs { |m,n| [n.key, m.val] }

We need to make this multicast conditional on the server's belief that it is the current leader:

    chatter <~ (chatter * nodelist * leader).combos do |m, n, l|
      if l.addr == ip_port and n.key != ip_port
        [n.key, m.val]
      end
    end

A node will relay a message if it believes it is the leader -- and will relay it to everyone except itself (after all,
it has already received the message).

The distributed systems part
------------------

Now comes the fun part.  How does a node know if it is the leader, and how does a non-leader know who the leader is?  How does this knowledge
persist or change under delay and failure?  We need a notion of *group membership*, and some form of *leader election*.  These are tricky things
to get right in general, but since in a chat application we require only best-effort behavior (e.g., if I send a message and no one sees it, I am OK with
attempting to send it again, so I do not require reliable message delivery) we can roll ourselves very simple versions of both.

First, we need to define the *leader* collection, as well as any internal collections needed to define it:

  state do
    periodic :interval, 1
    channel :heartbeat, [:@to, :from]
    table :recently_seen, heartbeat.key_cols + [:rcv_time]
    scratch :live_nodes, [:addr]
    interface output, :leader, [:addr]
  end
  
*interval* is a periodic ephemeral relation that will contain a tuple roughly once per second: we will use it to trigger *heartbeat* messages among nodes:

    heartbeat <~ (interval * nodelist).rights{|n| [n.key, ip_port]}

Every node sends every other node that it knows about a heartbeat message every second.  But how does it know about other nodes?  Recall that in the original chatserver, the server inserts an address into *nodelist* whenever it gets a *connect* message.

    nodelist <= connect{|c| [c.client, c.nick]}

