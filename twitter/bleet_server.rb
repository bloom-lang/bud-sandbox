require 'rubygems'
require 'backports'
require 'bud'
require 'http-cookie'
require 'readline'
require_relative './bleet_protocol'

class BleetServer
  include Bud
  include BleetProtocol

  state do
  	scratch :register,       [:from, :username, :pw, :command]
  	scratch :login,          [:from, :username, :pw, :command]
  	scratch :logout,         [:cook, :from] => [:username, :command]
  	scratch :follow,         [:from, :cook, :lead] => [:username, :command]
  	scratch :post,           [:cook, :text, :from] => [:username, :command]
  	scratch :getfeed,        [:cook, :from] => [:username, :command]

  	sync :registry, :dbm,    [:username] => [:pw]
  	sync :logged_in, :dbm,   [:cook] => [:addr, :username]
    sync :follower, :dbm,    [:lead, :follow]
  	sync :posts, :dbm,       [:seq_id] => [:username, :text]
  	sync :post_sequence, :dbm,    [:val]

  	scratch :new_follow,     [:from] + follower.schema + [:command]
  	scratch :reg_success,    register.schema
  	scratch :login_success,  [:cook, :addr, :username, :command]
  	table :login_cmds,       [:name]
  	table :commands,         [:name]
  	scratch :command_login,  [:cook, :from, :name, :params, :username]
  end

  bootstrap do
  	post_sequence <+ [[0]] if post_sequence.empty?  # initialize counter for posts
    login_cmds <+ LOGIN_COMMANDS                    # command names that require login 
    commands <+ LOGIN_COMMANDS                      # all commands
    commands <+ COMMANDS
  end

  # take in the command channel and demultiplex it to appropriate scratch tables
  bloom :demux do
  	register <= command_chan { |c| [c.from, c.params.split[0], c.params.split[1], c.name] if c.name == 'register' }
    login <= command_chan { |c| [c.from, c.params.split[0], c.params.split[1], c.name] if c.name == 'login' }
    register <= command_chan { |c| 
    	[ip_port, c.params.split[0], c.params.split[1], c.name] if c.name == 'register' 
    }
    # most commands require a login cookie. if the last output field is nil?, then the cookies weren't found
    command_login <= (command_chan*logged_in).outer(:cook => :cook) {|c,l|
    	[c.cook, c.from, c.name, c.params, l.username]
    }
    logout <= command_login { |c| 
    	[c.cook, c.from, c.username, c.name] if c.name == 'logout' 
    }
    follow <= command_login {|c| 
    	[c.from, c.cook, c.params.split[0], c.username, c.name] if c.name == 'follow'
    }
    post <= command_login {|c| 
    	[c.cook, c.params, c.from, c.username, c.name] if c.name == 'post'
    }
    getfeed <= command_login {|c| 
    	[c.cook, c.from, c.username, c.name] if c.name == 'getfeed'
    }
    # login_commands require you to be logged in. warn on failure.
    command_resp <~ (command_login*login_cmds).outer(:name => :name) {|c, l|
    	[c.cook, c.from, nil, "not logged in, " + c.name] if (c.username.nil? && !l.name.nil?)
    }    
    # default: command unknown
    command_resp <~ command_chan.notin(commands, :name => :name).pro {|c|
    	[c.cook, c.from, nil, "unknown command " + c.name]
    }
  end

  bloom :register do 
    # no preexisting match? good news!
  	reg_success <= register.notin(registry, :username => :username)
    registry <+ reg_success { |c| [c.username, c.pw] }
    command_resp <~ reg_success { |r| [nil, r.from, r.username, r.command] }
    # if you find a preexisting match, return the bad news
    command_resp <~ (register*registry).pairs(:username => :username) { |newy,old| 
    	[nil, newy.from, nil, newy.command] 
    }
  end

  bloom :login do
    # outer join means the username field will be nil if no match found in registry
  	login_success <= (login * registry).outer(:username => :username, :pw => :pw) { |l,r|
  		[HTTP::Cookie.new(l.from+l.username, SecureRandom.uuid, :domain=>l.from.split(':')[0]).to_s, 
  		 l.from, r.username, l.command]
  	}
    logged_in <+ login_success {|l| [l.cook, l.addr, l.username] unless l.username.nil?}
    command_resp <~ login_success # client to check username for the case where no match was found
  end

  bloom :logout do
    logged_in <- (logged_in * logout).lefts(:username => :username) {|l| l}
    command_resp <~ logout
  end

  bloom :follow do
  	new_follow <= (follow*registry).outer(:lead => :username) {|c, l|
    	[c.from, c.lead, c.username, c.command]	
    }

  	follower <= new_follow {|f| [f.lead, f.follow] unless f.follow.nil?}
    command_resp <~ new_follow {|f| [nil, f.from, f.follow, f.command]}
  end

  bloom :post do
  	posts <= post {|p| [post_sequence.first.val + 1, p.username, p.text]}
  	post_sequence <+ (post_sequence*post).lefts {|p| [p.val + 1] }
  	post_sequence <- (post_sequence*post).lefts
  	command_resp <~ post { |p| [nil, p.from, p.username, p.command] }
  end

  bloom :getfeed do
  	# posts I made
  	getfeed_resp <~ (getfeed*posts).pairs(:username => :username) {|g, p|
  		[nil, g.from, p.username, p.text, g.command]
  	}
  	# posts I follow
  	getfeed_resp <~ (getfeed*follower*posts).combos(getfeed.username => follower.follow,
  		                                             follower.lead => posts.username) {|g,f,p|
      [nil, g.from, p.username, p.text, g.command]
  	}
  end
end

# Provide a debugging shell that simply allows dumping state of tables
DBM_BUD_DIR = "#{Dir.pwd}/bud_tmp"
addr = ARGV.first ? ARGV.first : BleetProtocol::DEFAULT_ADDR
ip, port = addr.split(":")
puts "Server address: #{ip}:#{port}"
program = BleetServer.new(:ip => ip, :port => port.to_i, :dbm_dir => DBM_BUD_DIR)
program.run_bg

while buf = Readline.readline("enter a name to dump> ", true)
  begin
	program.sync_do # tick to capture prior activity
  rescue

  end
  if program.tables.has_key? buf.to_sym
	tups = program.tables[buf.to_sym].to_a.sort
  	puts(tups.empty? ? "(empty)" : tups.sort.map{|t| "#{t}"}.join("\n"))
  elsif program.lattices.has_key? buf.to_sym
    val = program.lattices[buf.to_sym].current_value
    puts val.inspect
  end
end
