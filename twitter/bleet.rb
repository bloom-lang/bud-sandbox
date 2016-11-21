require 'readline'
require 'rubygems'
require 'backports'
require 'bud'
require 'stringio' 
require_relative 'bleet_protocol'

class ChatClient
  include Bud
  include BleetProtocol

  state do
    interface input, :bleet_in, [:line]
    scratch :command, [:name, :params]
    table :session, [:cooky]
  end

  def initialize(nick, server, opts={})
    @nick = nick
    @server = server
    super opts
  end

  bloom :commands do
    command <= bleet_in { |s| s.line.split(' ', 2) }
    # use outer join since some commands don't require a session cookie
    command_chan <~ (command * session).outer {|c, s| [@server, ip_port, c.name, c.params, s.cooky]}
    session <= command_resp {|l| [l.cooky] if l.command == 'login'}
    session <- command_resp { |l| [l.cooky] if l.command == 'logout'}
  end

  bloom :responses do
    stdio <~ command_resp {|r| [pretty_print(r.addr, r.command, 
                                (r.succeeded.nil? ? " failed" : " succeeded"))] }
    stdio <~ getfeed_resp{|r| [pretty_print(r.poster, r.text)]}
  end
end

server = (ARGV.length == 2) ? ARGV[1] : BleetProtocol::DEFAULT_ADDR
puts "Server address: #{server}"
out_read, out_write = IO.pipe
bleet = ChatClient.new(ARGV[0], server, :stdout => out_write)
bleet.run_bg

# simple Ruby command-line client
while buf = Readline.readline("bleet> ", true)
  bleet.sync_do{ bleet.bleet_in <+ [[buf]] }
  begin
    Timeout::timeout(0.5) {
      while true
        puts out_read.readline
      end
    }
  rescue
  end
end
