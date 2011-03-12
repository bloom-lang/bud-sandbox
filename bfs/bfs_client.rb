require 'rubygems'
require 'bud'
require 'bud/rendezvous'
require 'backports'
require 'timeout'
require 'bfs/bfs_client_proto'
require 'bfs/data_protocol'
require 'bfs/bfs_config'

# The BFS client and shell stand between ruby and BUD.  BSFShell provides dispatch_command() as a synchronous functional interface
# for FS operations


module BFSClient
  include BFSClientProtocol
  include BFSClientMasterProtocol

  state do
    table :master, [] => [:master]
  end

  declare
  def cglue
    # every request involves some communication with the master.
    #stdio <~ request.map{|r| ["REQUEST: #{r.inspect}"] }
    #stdio <~ response.map{|r| ["response: #{r.inspect}"] }
    request_msg <~ join([request, master]).map{|r, m| [m.master, ip_port, r.reqid, r.rtype, r.arg] }
  
    response <= response_msg.map do |r|
      [r.reqid, r.status, r.response]
    end 
  end
end

class BFSShell
  include Bud
  include BFSClient
  
 def initialize(master)
    @master = master
    @queue = Queue.new
    # bootstrap?

    super(:dump => true)
    my_queue << [@queue]
  end

  state do
    table :remember_response, response.schema
    table :watched_ids, [:reqid]
    table :my_queue, [] => [:queue]
    scratch :hollow, [:straw]
  end

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
    reqid = UUID.new.to_s
    ren = Rendezvous.new(self, response)
    async_do{ request <+ [[reqid, op, args]] }
    res = ren.block_on(5)
    ren.stop
    if res[0] = reqid
      return res
    else 
      raise "GOT (wrong) RES #{res}" 
    end
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
    res[2].sort{|a, b| a <=> b}.each do |chk|
      res = synchronous_request(:getchunklocations, chk)
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
    sendlist = preflist.sort_by{rand}[0..REP_FACTOR-1]
    DataProtocolClient.send_stream(chunkid, sendlist, DataProtocolClient.chunk_from_fh(fh))
  end

  def do_ls(args)
    res = synchronous_request(:ls, args[0])
    return res[2]
  end
end
