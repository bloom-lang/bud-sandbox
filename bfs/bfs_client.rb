require 'rubygems'
require 'bud'
require 'backports'
require 'timeout'
require 'bfs/bfs_client_proto'
require 'bfs/data_protocol'

module BFSClient
  include BFSClientProtocol

  state {
    table :master, [] => [:master]
  }

  declare
  def cglue
    # every request involves some communication with the master.
    request_msg <~ join([request, master]).map{|r, m| [m.master, ip_port, r.reqid, r.rtype, r.arg] }
  
    response <= response_msg.map do |r|
      [r.reqid, r.status, r.response]
    end 
    #stdio <~ response.map{|r| ["response: #{r.inspect}"] }
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
  def synchronization
    #remember_response <= response.map{|r| puts "remember #{r.inspect}, with watched_ids #{watched_ids.length} and my_queue #{my_queue.length}" or r }
    remember_response <= response
    snc = join [remember_response, watched_ids, my_queue], [remember_response.reqid, watched_ids.reqid]
    hollow <= snc.map do |r, w, q|
      #puts "Enqueue #{r.inspect} (on a Q of length #{q.length})" or [q.queue.push r]
      [q.queue.push r]
    end
    watched_ids <- snc.map{ |r, w| w }
    remember_response <- snc.map{ |r, w| r }
  end
  
  bootstrap do
    master << [@master]
  end

  def dispatch_command(args, filehandle=nil)
    #puts "args is #{args} type #{args.class} len #{args.length}"
    op = args.shift
    #puts "op is #{op} class #{op.class}"
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
    #puts "got file #{file}, path #{path}"
    reqid = 1 + rand(10000000)
    if is_dir
      #puts "MKDIR TIME"
      sync_do{ request <+ [[reqid, :mkdir, [file, path]]] }
    else 
      sync_do{ request <+ [[reqid, :create, [file, path]]] }
    end
  end

  def do_mkdir(args)
    do_create(args, true)
  end

  def do_read(args)
    reqid = 1 + rand(10000000)
    sync_do{ request <+ [[reqid, :getchunks, args[0]]] }
    res = slightly_less_ugly(reqid)
    res.response.sort{|a, b| a <=> b}.each do |chk|
      reqid = 1 + rand(10000000)
      sync_do{ request <+ [[reqid, :getchunklocations, chk]] }
      res = slightly_less_ugly(reqid)
      chunk = DataProtocolClient.read_chunk(chk, res[2])
      puts chunk
    end
  end

  def do_append(args, fh)
    ret = true
    while ret
      #puts "do a chunk"
      ret = do_a_chunk(args, fh)
    end
    #puts "DONE APPENDING"
  end
  
  def do_a_chunk(args, fh)
    reqid = 1 + rand(10000000)
    sync_do{ request <+ [[reqid, :append, args[0]]] }
    # block for response....
    ret = slightly_less_ugly(reqid)
    raise "add chunk metadata failed: #{ret.inspect}" unless ret[1]
    
    chunkid = ret[2][0]
    preflist = ret[2][1]
    #puts "chunkid is #{chunkid}.  preflist is #{preflist}"
    DataProtocolClient.send_stream(chunkid, preflist, DataProtocolClient.chunk_from_fh(fh))
  end

  def slightly_less_ugly(reqid)
    sync_do { watched_ids <+ [[reqid]] }
    sync_do {}
    #puts "WAIT"
    res = nil
    Timeout::timeout(5) do
      res = @queue.pop
    end
    #puts "DONE waiting for #{reqid}, POPPED OFF #{res.inspect}! "
    if res.reqid != reqid
      #puts "ahem, popped off the wrong id...  chucking it."
      res = slightly_less_ugly(reqid)
    end
    return res
  end

  def do_ls(args)
    reqid = 1 + rand(10000000)
    sync_do{ request <+ [[reqid, :ls, args[0]]] }
    res = slightly_less_ugly(reqid)
    return res.response
  end

end


#s = BFSShell.new
#s.dispatch_command(ARGV)

#sleep 5
