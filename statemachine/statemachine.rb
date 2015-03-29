module StateMachine
	state do
	    table   :states, [:name] => [:accepts]   
	    table   :xitions, [:from, :to, :event]
	    table   :current, [] => [:name] 
	    scratch :event, [:name]
	end

	bootstrap do
		current <= states {|s| [s.name] if s.name == 'start'}
	end

	bloom do
		# change upon legal transition
		current <+- (current*event*xitions).combos(current.name=>xitions.from, 
			                                       event.name=>xitions.event) {|c,e,x|
			[x.to]
		}
		
		# reset current 
		current <+- event {|e| ['start'] if e.name == 'reset'}
	end
end