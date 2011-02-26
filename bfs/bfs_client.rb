require 'rubygems'
require 'bud'
require 'backports'
require 'timeout'
require 'bfs/bfs_client_proto'
require 'bfs/data_protocol'

# The BFS client and shell stand between ruby and BUD.  BSFShell provides dispatch_command() as a synchronous functional interface
# for FS operations


module BFSClient
  include BFSClientProtocol

  state {
    table :master, [] => [:master]
  }

  declare
  def cglue
    # every request involves some communication with the master.
  
    stdio <~ request.map{|r| ["REQUEST: #{r.inspect}"] }
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
    remember_response <= response
    snc = join [remember_response, watched_ids, my_queue], [remember_response.reqid, watched_ids.reqid]
    hollow <= snc.map do |r, w, q|
      [q.queue.push r]
    end
    watched_ids <- snc.map{ |r, w| w }
    remember_response <- snc.map{ |r, w| r }
  end
  
  bootstrap do
    master << [@master]
  end

  def dispatch_command(args, filehandle=nil)
    op = args.shift
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
        do_read(args, (filehandle.nil? ? STDOUT : filehandle))
      when "rm"
        do_rm(args)
      else
        raise "unknown op: #{op}"
    end
  end
  
  def get_base_and_path(path)
    items = path.split("/")
    return [items.pop, items.length == 1 ? "/" : items.join("/")]
  end 

  def synchronous_request(op, args)
    reqid = 1 + rand(10000000)
    sync_do{ request <+ [[reqid, op, args]] }
    return slightly_less_ugly(reqid)
  end 

  def do_createfile(args)
    do_create(args, false)
  end

  def do_create(args, is_dir)
    (file, path) = get_base_and_path(args[0])
    if is_dir
      synchronous_request(:mkdir, [file, path])
    else 
      synchronous_request(:create, [file, path])
    end
  end

  def do_mkdir(args)
    do_create(args, true)
  end

  def do_rm(args)
    (file, path) = get_base_and_path(args[0])
    synchronous_request(:rm, [file, path]) 
  end

  def do_read(args, fh)
    res = synchronous_request(:getchunks, args[0])
    res.response.sort{|a, b| a <=> b}.each do |chk|
      reqid = 1 + rand(10000000)
      sync_do{ request <+ [[reqid, :getchunklocations, chk]] }
      res = slightly_less_ugly(reqid)
      chunk = DataProtocolClient.read_chunk(chk, res[2])
      fh.write chunk
    end
  end

  def do_append(args, fh)
    ret = true
    while ret
      ret = do_a_chunk(args, fh)
    end
  end
  
  def do_a_chunk(args, fh)
    ret = synchronous_request(:append, args[0])
    raise "add chunk metadata failed: #{ret.inspect}" unless ret[1]
    
    chunkid = ret[2][0]
    preflist = ret[2][1]
    #puts "chunkid is #{chunkid}.  preflist is #{preflist}"
    DataProtocolClient.send_stream(chunkid, preflist, DataProtocolClient.chunk_from_fh(fh))
  end

  def slightly_less_ugly(reqid)
    sync_do { watched_ids <+ [[reqid]] }
    sync_do {}
    res = nil
    Timeout::timeout(5) do
      res = @queue.pop
    end
    if res.reqid != reqid
      res = slightly_less_ugly(reqid)
    end
    return res
  end

  def do_ls(args)
    res = synchronous_request(:ls, args[0])
    return res.response
  end
end
