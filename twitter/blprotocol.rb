module BleetProtocol
  state do
  	channel          :command_chan,  [:@addr, :from, :name, :params, :cooky]
  	channel          :command_resp,  [:cooky, :@addr, :succeeded, :command]
  	channel          :getfeed_resp,  [:cooky, :@addr, :poster, :text, :command]
  end

  DEFAULT_ADDR = "127.0.0.1:12345"
  LOGIN_COMMANDS = [['logout'], ['follow'], ['post'], ['getfeed'], ['session']]
  NO_LOGIN_COMMANDS = [['register'], ['login']]
end

# Ruby code to format messages with color and timestamp on the right of the screen
# format is <val1>: <val2>      Time.now
def pretty_print(*args)
	return if args.nil?
	val = args.shift
	val = val.empty? ? '' : val
	if (val.nil? || args.length == 0)
		return ('')
	else
		rest = args.map {|v| v.nil? ? '' : v}
		rest = args.join('')
		str = "\033[34m"+val + ": " + "\033[31m" + rest + "\033[0m"
		pad = "(" + Time.now.to_s + ")"
		return str + " "*[66 - str.length,2].max + pad
	end
end