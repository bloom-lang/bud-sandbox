require 'rubygems'
require 'backports'
require 'bud'
require_relative 'blprotocol'

module BleetClient
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

  bloom do
    # pass client requests and session state to server
    command <= bleet_in { |s| s.line.split(' ', 2) }
    # use outer join since some commands don't require a session cookie
    command_chan <~ (command * session).outer {|c, s| [@server, ip_port, c.name, c.params, s.cooky]}

    # handle command responses from server
    session <= command_resp {|l| [l.cooky] if l.command == 'login'}
    session <- command_resp { |l| [l.cooky] if l.command == 'logout'}
    stdio <~ command_resp {|r| [pretty_print(r.addr, r.command, 
                                (r.succeeded.nil? ? " failed" : " succeeded"))] }
    stdio <~ getfeed_resp{|r| [pretty_print(r.poster, r.text)]}
  end
end
