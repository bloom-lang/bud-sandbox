require "rubygems"
require "bud"

$nodes = []
$addr_map = {}

class VcAgent
  include Bud

  state do
    scratch :kickoff, [] => [:v]
    scratch :done, [] => [:v]

    channel :chn, [:@addr, :msg, :delay, :clock]
    table :recv_buf, [:msg, :clock, :deliver_at_time]
    table :send_buf, [:addr, :msg, :delay, :send_at_time]
    scratch :sbuf_chosen, send_buf.schema
    scratch :rbuf_chosen, recv_buf.schema
    periodic :tik, 1

    lat_map :my_vc
    lat_map :next_vc
  end

  bootstrap do
    my_vc <= [[ip_port, MaxLattice.wrap(0)]]
  end

  bloom do
    stdio <~ rbuf_chosen {|r| ["#{@budtime} -- Delivering message @ #{port} (node # = #{$addr_map[ip_port]}), msg ID = #{r.msg}, msg clock = #{r.clock.inspected}, local VC = #{my_vc.inspected}"]}

    # Setup a specific messaging scenario: node 3 will (usually) receive message
    # 3 followed by message 1, violating causal order
    send_buf <= kickoff { [$nodes[2].ip_port, 100, 3, @budtime]}
    send_buf <= kickoff { [$nodes[1].ip_port, 101, 0, @budtime + 1]}
    send_buf <= chn {|c| [$nodes[2].ip_port, 102, 0, @budtime] if $addr_map[ip_port] == 1}

    sbuf_chosen <= send_buf {|s| s if s.send_at_time == @budtime}
    send_buf <- sbuf_chosen
    chn <~ sbuf_chosen {|s| [s.addr, s.msg, s.delay, next_vc]}

    # At the receiver, buffer messages and deliver w/ appropriate delay
    recv_buf <= chn {|r| [r.msg, r.clock, @budtime + r.delay]}
    rbuf_chosen <= recv_buf {|r| r if r.deliver_at_time == @budtime}
    recv_buf <- rbuf_chosen
    done <= rbuf_chosen {|r| [true] if ($addr_map[ip_port] == 2 and r.msg == 100)}

    # If there are any incoming or outgoing messages, bump the local VC; merge
    # local VC with VCs of incoming messages
    next_vc <= my_vc
    next_vc <= sbuf_chosen { [ip_port, my_vc[ip_port] + 1]}
    next_vc <= rbuf_chosen { [ip_port, my_vc[ip_port] + 1]}
    next_vc <= rbuf_chosen {|c| c.clock}
    my_vc <+ next_vc
  end
end

3.times do |i|
  b = VcAgent.new
  b.run_bg
  $addr_map[b.ip_port] = i
  $nodes << b
end

puts "Started: #{$nodes.map{|n| n.port}.inspect}"

q = Queue.new
$nodes.last.register_callback(:done) do |t|
  q.push(true)
end

n = $nodes.first
n.sync_do {
  n.kickoff <+ [[true]]
}

q.pop

$nodes.each do |n|
  n.sync_do     # Make sure that the final next_vc => my_vc merge occurs
  n.sync_do {
    puts "#{n.my_vc.inspected} @ #{n.ip_port}"
  }
  n.stop
end
