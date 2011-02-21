require 'rubygems'
require 'bud'
require 'backports'
require 'bfs/bfs_client_proto'

CHUNKSIZE = 100000

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
      puts "RESP" or [r.reqid, r.status, r.response]
    end 
    stdio <~ response.map{|r| ["response: #{r.inspect}"] }
  end
end

class BFSShell
  include Bud
  include BFSClient
  
 def initialize(master)
    @master = master
    @queue = Queue.new
    # bootstrap?

    super(:dump => true)#, :visualize => 3)
    my_queue << [@queue]
  end

  
  state {
    table :remember_response, response.schema
    table :watched_ids, [:reqid]
    table :my_queue, [] => [:queue]
    scratch :hollow, [:straw]
  }

  declare
  def persistence
    remember_response <= response.map{|r| puts "remember #{r.inspect}, with watched_ids #{watched_ids.length} and my_queue #{my_queue.length}" or r }

    snc = join [remember_response, watched_ids, my_queue], [remember_response.reqid, watched_ids.reqid]
    hollow <= snc.map do |r, w, q|
      puts "Enqueue #{r.inspect}" or [q.queue.push r]
    end
    watched_ids <- snc.map{ |r, w| w }
  end
  
  def bootstrap
    master << [@master]
    #my_queue << [@queue]
    super
  end

  def dispatch_command(args, filehandle=nil)
    puts "args is #{args} type #{args.class} len #{args.length}"
    op = args.shift
    puts "op is #{op} class #{op.class}"
    case op
      when "ls" then
        do_ls(args)
      when "create"
        do_createfile(args)
      when "append"
        do_append(args, (filehandle.nil? ? STDIN : filehandle))
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

  def do_append(args, fh)
    ret = true
    while ret
      puts "do a chunk"
      ret = do_a_chunk(args, fh)
    end
    puts "DONE APPENDING"
  end
  
  def do_a_chunk(args, fh)
    reqid = 1 + rand(10000000)
    sync_do{ request <+ [[reqid, :append, args[0]]] }
    # block for response....
    ret = slightly_less_ugly(reqid)
    puts "response is #{ret.inspect}"
    raise "add chunk metadata failed" unless ret[1]
    chunkid = ret[2][0]
    preflist = ret[2][1]
    puts "chunkid is #{chunkid}.  preflist is #{preflist}"
    send_stream(chunkid, preflist, fh)
  end

  def send_stream(chunkid, prefs, fh)
    copy = prefs.clone
    first = copy.shift
    host, port = first.split(":")
    copy.unshift(chunkid) 
    s = TCPSocket.open(host, port)
    s.puts(copy.join(","))
    chunk = fh.read(CHUNKSIZE) 
    if chunk.nil?
      s.close
      return false
    else
      s.write(chunk)
      s.close
      return true
    end
  end

  def slightly_less_ugly(reqid)
    puts "SLu"
    sync_do { watched_ids <+ [[reqid]] }
    puts "WAIT"
    sync_do {}
    sync_do {}
    res = @queue.pop
    puts "POPPED OFF #{res.inspect}! "
    return res
  end

  def do_ls(args)
    reqid = 1 + rand(10000000)
    sync_do{ request <+ [[reqid, :ls, args[0]]] }
  end

end


#s = BFSShell.new
#s.dispatch_command(ARGV)

#sleep 5
