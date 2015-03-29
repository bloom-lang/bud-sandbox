# to use the state machine, make sure to issue a "reset" event first
module StateMachine
	state do
	    table   :states, [:name] => [:accepts]   
	    table   :xitions, [:from, :to, :event]
	    table   :current, [] => [:name] 
	    interface input, :event, [:name]
	    interface output, :result, [:accepted]
	end

	bloom do
		# result is the acceptance flag of current state
		result <= (current*states).pairs(current.name=>states.name) {|c,s| [s.accepts]}

		# change upon legal transition
		current <+- (current*event*xitions).combos(current.name=>xitions.from, 
			                                       event.name=>xitions.event) {|c,e,x|
			[x.to]
		}
		
		# reset current 
		current <+- event {|e| ['start'] if e.name == 'reset'}
	end
end