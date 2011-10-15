require "rubygems"
require "bud"

$nodes = []
$addr_map = {}

class VcAgent
  include Bud

  state do
    scratch :kickoff, [] => [:v]
    scratch :done, [] => [:v]

    channel :chn, [:@addr, :msg, :from, :clock]
    table :send_buf, [:addr, :msg, :send_at_time]
    scratch :buf_chosen, send_buf.schema
    periodic :tik, 1

    lat_map :my_vc
    lat_map :next_vc
  end

  bootstrap do
    my_vc <= [[ip_port, MaxLattice.wrap(0)]]
  end

  bloom do
    stdio <~ chn {|c| ["Got message @ #{port} (node # = #{$addr_map[ip_port]}), msg ID = #{c.msg}, msg clock = #{c.clock.inspected}, local VC = #{my_vc.inspected}"]}

    # Setup a specific messaging scenario: node 3 will (usually) receive message
    # 3 followed by message 1, violating causal order
    send_buf <= kickoff { [$nodes[2].ip_port, 1, @budtime + 2]}
    send_buf <= kickoff { [$nodes[1].ip_port, 2, @budtime]}
    send_buf <= chn {|c| [$nodes[2].ip_port, 3, @budtime] if $addr_map[ip_port] == 1}

    buf_chosen <= send_buf {|s| s if s.send_at_time == @budtime}
    send_buf <- buf_chosen
    chn <~ buf_chosen {|s| [s.addr, s.msg, $addr_map[ip_port], next_vc]}
    done <= chn {|c| [true] if ($addr_map[ip_port] == 2 and c.msg == 1)}

    # When we send or receive a message, bump the local VC; merge local VC with
    # VCs of incoming messages
    next_vc <= my_vc
    next_vc <= buf_chosen { [ip_port, MaxLattice.wrap(my_vc[ip_port].reveal + 1)]}
    next_vc <= chn { [ip_port, MaxLattice.wrap(my_vc[ip_port].reveal + 1)]}
    next_vc <= chn {|c| c.clock}
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
  n.sync_do {
    puts n.my_vc.inspected
  }
  n.stop
end
