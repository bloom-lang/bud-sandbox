Peer to peer, fault-tolerant chat
=========================

Writing a simple chat server in Bloom is extremely [easy](https://github.com/bloom-lang/bud/tree/master/examples/chat).  The job of a chat [client](https://github.com/bloom-lang/bud/blob/master/examples/chat/chat.rb) is to forward messages (typed into the keyboard) to a central server, and to print (to the screen) messages relayed by that server.  The job of a [server](https://github.com/bloom-lang/bud/blob/master/examples/chat/chat_server.rb) is to maintain a list of members, and forward all messages to all members.

In this short demo, we will evolve that toy chat server into a distributed system that is *decentralized* and *fault-tolerant*.  When we are finished, we will have a chat program that behaves essentially the same as the original, except that 1) all nodes can play the role of client or server, and 2) when the server node fails,
one of the clients automatically assumes its role.

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

First, we need to define the *leader* collection, as well as any internal collections needed to describe what it contains:

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

Now all nodes have this rule, so we can reuse *connect* as a mechanism to update *nodelist*.  We make sure that all nodes eventually have an _overestimate_ of the set of active nodes by promiscuously broadcasting the leader's nodelist:

    connect <~ (interval * nodelist * nodelist).combos do |h, n1, n2|
      [n1.key, n2.key, n2.val]
    end
    
    
Now nodes with information share the information actively, so each node can (probably) independently determine the group membership and the current leader.
The group membership -- the current set of live nodes, roughly -- is a view over the heartbeat log:

    recently_seen <= heartbeat{|h| h.to_a + [Time.now.to_i]}
    live_nodes <= recently_seen.group([:from], max(:rcv_time)) do |n|
      [n.first] unless  (Time.now.to_i - n.last > 3)
    end
    
The live nodes are those nodes from whom we have recently received heartbeat messages.

The rules defining *leader* are a view over that view:

    leader <= live_nodes.group([], min(:addr))

The leader is the live node with the lowest address.  That's it!

The code
----------

... is very [concise](https://github.com/bloom-lang/bud-sandbox/blob/master/chat/chat.rb).


Running it
------------
To bootstrap clients with a *nodelist*, we need to tell them the address of the current server when we launch the program.
If (perhaps due to a leader race), we instantiate a client with the address of a node that is not the current leader, that is ok.
The node that receives the *connect* message will add the new node to its *nodelist*, and later forward its best guess at the current *nodelist* to the new node.
At this point, the new node can determine the current leader and begin relaying messages to it.

We include a simple [wrapper](https://github.com/bloom-lang/bud-sandbox/blob/master/chat/single.rb) for running chat from the commandline on a single node, for demonstration purposes.
In a given console, you can run it like:

    > ruby simple.rb NICKNAME LEADER_PORT [MY_PORT]
    
For example,

    console1> ruby simple.rb peter 1234 1234
    console2> ruby simple.rb paul 1234 2345
    console3> ruby simple.rb mary 1234 3456
    
    
