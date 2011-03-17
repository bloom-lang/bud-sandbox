require 'rubygems'
require 'bud'
require 'bud/rendezvous'
require 'backports'
require 'timeout'
require 'bfs/bfs_client_proto'
require 'bfs/data_protocol'
require 'bfs/bfs_config'
require 'timeout'

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

  def BFSClient::gen_id
    UUID.new.to_s
  end
end

class BFSClientError < RuntimeError
end

class BFSShell
  include Bud
  include BFSClient
  
 def initialize(master, opts = {})
    @master = master
    super(opts)
  end

  state do
    table :remember_response, response.schema
    table :watched_ids, [:reqid]
    table :my_queue, [] => [:queue]
    scratch :hollow, [:straw]
  end

  bootstrap do
    master << [@master]
  end

  def dispatch_command(args, filehandle=nil)
    (0..CLIENT_RETRIES).each do |i|
      begin
        return dispatch_command_internal(args, filehandle)
      rescue BFSClientError, Timeout::Error
        puts "Error: #{$!}"
        sleep 1
        puts "retrying..." if i < CLIENT_RETRIES
      end
    end
    raise
  end

  def dispatch_command_internal(args, filehandle=nil)
    copy = args.clone
    op = copy.shift
    case op
      when "ls" then
        do_ls(copy)
      when "create"
        do_createfile(copy)
      when "append"
        do_append(copy, (filehandle.nil? ? STDIN : filehandle))
      when "mkdir"
        do_mkdir(copy)
      when "read"
        do_read(copy, (filehandle.nil? ? STDOUT : filehandle))
      when "rm"
        do_rm(copy)
      else
        raise "unknown op: #{op}"
    end
  end
  
  def get_base_and_path(path)
    items = path.split("/")
    return [items.pop, items.length == 1 ? "/" : items.join("/")]
  end 
  
  def synchronous_request(op, args)
    reqid = gen_id
    ren = Rendezvous.new(self, response)
    async_do{ request <+ [[reqid, op, args]] }
    begin
      res = ren.block_on(15)
    rescue
      raise BFSClientError, $!
    end
    ren.stop
    if res[0] = reqid
      return res
    else 
      raise BFSClientError, "GOT (wrong) RES #{res}" 
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
    begin 
      res = synchronous_request(:getchunks, args[0])
      res[2].sort{|a, b| a <=> b}.each do |chk|
        res = synchronous_request(:getchunklocations, chk)
        chunk = DataProtocolClient.read_chunk(chk, res[2])
        fh.write chunk
      end
    rescue
      raise BFSClientError, $!
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
    raise BFSClientError, "add chunk metadata failed: #{ret.inspect}" unless ret[1]
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
