require './chat.rb'

# a name, a server to attemp to connect to to bootstrap, and (optionally) a port to bind to.
# for this 1-computer demo, let's save ourselves some typing and leave off the ip part: just ports.

port = (ARGV.length == 3) ? ARGV[2] : Socket::INADDR_ANY

program = SingleChat.new(ARGV[0], "127.0.0.1:#{ARGV[1]}", :stdin => $stdin, :port => port)
program.run_fg


