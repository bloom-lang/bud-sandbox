require 'rubygems'
require 'bud'
require 'backports'
require 'heartbeat/heartbeat'
require 'membership/membership'
require 'bfs/data_protocol'

module BFSDatanode
  include HeartbeatAgent
  include StaticMembership

  state do
    scratch :dir_contents, [:file, :time]
    table :last_dir_contents, [:file, :time]
    scratch :to_payload, [:file, :time]
  end

  declare 
  def hblogic
    dir_contents <= hb_timer.flat_map do |t|
      dir = Dir.new("#{DATADIR}/#{@data_port}")
      files = dir.to_a.map{|d| d.to_i unless d =~ /^\./}.uniq!
      dir.close
      files.map {|f| [f, Time.parse(t.val).to_f]}
    end

    to_payload <= dir_contents.map do |c|
      c unless last_dir_contents.map{|l| l.file}.include? c.file
    end

    #jdc = join([dir_contents, last_dir_contents], [dir_contents.file, last_dir_contents.file])
    #payload <= jdc.map do |c, l|
    #  # every 10 seconds we resend the whole list
    #  if c.time - l.time > 10
    #    puts "REDO COMPLETE: #{c}" or c
    #  end
    #end
    #last_dir_contents <- jdc.map {|c, l| l}


    #last_dir_contents <- join([last_dir_contents, hb_timer]).map do |c, t|
    #last_dir_contents <- join([last_dir_contents, hb_timer]).map do |c, t|
    last_dir_contents <- join([hb_timer, last_dir_contents]).map do |t, c|
      #c if (Time.parse(t.val).to_f - c.time) > 10
      if (Time.parse(t.val).to_f - c.time) > 4
        puts "RESEND" or c
      else
        []
      end
    end

    last_dir_contents <+ to_payload
    last_dir_contents <- join([to_payload, last_dir_contents], [to_payload.file, last_dir_contents.file]).map {|d, l| l}

    stdio <~ payload.inspected
    payload <= to_payload.group(nil, accum(to_payload.file))
  end

  def initialize(dataport=nil, opts={})
    super(opts)
    @data_port = dataport.nil? ? 0 : dataport
    @dp_server = DataProtocolServer.new(dataport)
    return_address <+ [["localhost:#{dataport}"]]
  end

  def stop_datanode
    @dp_server.stop_server
    stop_bg
  end
end
