import java.util.*;


/** Description of MC State class
 * @author Jose Monsalve
 * 
 */
public class MC_State {
	private
		/** Counter field contains how many times this element appeared in the training of the Markov Chain*/
		Integer counter;
		/** stateName is just an readable identifier for the state*/
		String stateName;
		/** The other states map contains the edges between different states. The value is the counter of how many times this transition has been made*/
		HashMap <MC_State,Integer> otherStates;
	public
		/** Constructor for the MC_State*/
		MC_State()
		{
			otherStates=new HashMap<MC_State, Integer>();
			counter = new Integer(0);
			stateName= new String();
		}
		/** Getter for the counter*/
		int getCounter()
		{
			return this.counter;
		}
		/** Setter for the counter */
		void setCounter(int counter)
		{
			this.counter=counter;
		}
		/** Getter for the StateName*/
		String getStateName()
		{
			return this.stateName;
		}
		/** Setter for the stateName*/
		void setStateName(String stateName)
		{
			this.stateName=stateName;
		}
		
		/** Increment the counter by amount
		 * @param amount counter = counter + amount
		 */
		void increaseCounter(int amount)
		{
			this.counter+=amount;
		}
		/** Increment the counter by one for a transition between two states. If the transition does not exist, it add it to the map. 
		 * @param next Is the object {@link MC_State} that represents the next state 
		 */
		void increaseTransition(MC_State next)
		{
			if (this.otherStates.containsKey(next))
			{
				Integer transition = this.otherStates.get(next);
				transition= new Integer(transition+1);
				this.otherStates.put(next,transition);
			}
			else
			{
				Integer transition = new Integer(1);
				this.otherStates.put(next, transition);
			}
		}
		
		/** Increment the counter by one for a transition between two states. If the transition does not exist, it add it to the map. 
		 * @param next Is the object {@link MC_State} that represents the next state 
		 */
		float getTransitionProb(MC_State next)
		{
			if (this.otherStates.containsKey(next))
			{
				float prob=((float)this.otherStates.get(next))/((float)this.counter);
				return prob;
			}
			return 0;
		}
}
