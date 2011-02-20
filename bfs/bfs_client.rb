require 'rubygems'
require 'bud'
require 'bfs/bfs_client_proto'

module BFSClient
  include BFSClientProtocol

  state {
    table :master, [] => [:master]
  }

  declare
  def cglue
    # every request involves some communication with the master.
    request_msg <~ join([request, master]).map{|r, m| puts "send rm" or [m.master, ip_port, r.reqid, r.rtype, r.arg] }
  
    response <= response_msg.map do |r|
      [r.reqid, r.status, r.response]
    end 
    stdio <~ response.map{|r| ["response: #{r.inspect}"] }
  end
end

class BFSShell
  include Bud
  include BFSClient
  
 def initialize(master)
    @master = master
    super()
  end
  
  def bootstrap
    master << [@master]
  end

  def dispatch_command(args)
    puts "args is #{args} type #{args.class} len #{args.length}"
    op = args.shift
    puts "op is #{op} class #{op.class}"
    case op
      when "ls" then
        do_ls(args)
      when "create"
        do_createfile(args)
      when "append"
        do_append(args)
      when "mkdir"
        do_mkdir(args)
      when "read"
        do_read(args)
      else
        raise "unknown op: #{op}"
    end
  end
  
  def get_base_and_path(path)
    items = path.split("/")
    return [items.pop, items.length == 1 ? "/" : items.join("/")]
  end  

  def do_createfile(args)
    do_create(args, false)
  end

  def do_create(args, is_dir)
    file = args[0]
    (file, path) = get_base_and_path(args[0])
    puts "got file #{file}, path #{path}"
    reqid = 1 + rand(10000000)
    if is_dir
      puts "MKDIR TIME"
      sync_do{ request <+ [[reqid, :mkdir, [file, path]]] }
    else 
      sync_do{ request <+ [[reqid, :create, [file, path]]] }
    end
  end

  def do_mkdir(args)
    do_create(args, true)
  end

  def do_append(args)
    reqid = 1 + rand(10000000)
    sync_do{ request <+ [[reqid, :append, args[0]]] }
    # block for response....
    # replace polling with something watch-based?
    ret = nil
    # PAA
    sleep 4
    return
    sync_do { 
      ready = false
      while !ready do
        response.each do |r|
          if r.reqid == reqid
            puts "got resp: #{r.inspect}"
            ready = true
          else 
            puts "WAIT"
            sleep 0.3
          end
        end 
      end  
    }

  end

  def do_ls(args)
    reqid = 1 + rand(10000000)
    sync_do{ request <+ [[reqid, :ls, args[0]]] }
  end

end


#s = BFSShell.new
#s.dispatch_command(ARGV)

#sleep 5
