
import java.util.*;
/**Class Markov Chain 
 * @author Jose Monsalve
 * */
public class MarkovChain {
	private
		/** The HashMap States */
		HashMap<String,MC_State> States;
		/** InitialState*/
		MC_State InitialState;
	public
		MarkovChain()
		{
			States=new HashMap<String,MC_State>();
		}
		void insertOrIncrementState(String State)
		{
			if(this.States.containsKey(State))
			{
				MC_State state = this.States.get(State);
				state.increaseCounter(1);
			}
			else
			{
				MC_State newState = new MC_State();
				newState.increaseCounter(1);
				newState.setStateName(State);
				this.States.put(State, newState);
			}
		}
		void IncrementTransition(String Origin, String Destination)
		{
			if(!this.States.containsKey(Origin))
			{
				MC_State newState = new MC_State();
				newState.increaseCounter(1);
				newState.setStateName(Origin);
				this.States.put(Origin, newState);
				
			}
			if(!this.States.containsKey(Destination))
			{
				MC_State newState = new MC_State();
				newState.increaseCounter(1);
				newState.setStateName(Destination);
				this.States.put(Destination, newState);
			}
			MC_State originState = this.States.get(Origin);
			MC_State destState = this.States.get(Destination);
			originState.increaseTransition(destState);
		}
		void PrintTransitionMatrix()
		{
			float TM[][]=new float[this.States.size()][this.States.size()];
			
			//We create a hashmap that contains the pair state, index of the table
			ArrayList<MC_State>StatesArray = new ArrayList <MC_State>();
			
			Iterator It = this.States.entrySet().iterator();
			for(MC_State value: this.States.values())
			{
				StatesArray.add (value);
			}
			
			System.out.printf("\t");
			for (int i = 0 ; i < StatesArray.size() ; i ++)
			{
				System.out.printf("%s\t", StatesArray.get(i).getStateName());
			}
			System.out.printf("\n");
			for (int i = 0 ; i<StatesArray.size() ; i++)
			{
				System.out.printf("%s\t",StatesArray.get(i).getStateName());
				for (int j = 0 ; j < StatesArray.size(); j++)
				{
					TM[i][j]=StatesArray.get(i).getTransitionProb(StatesArray.get(j));
					System.out.printf("%f\t",TM[i][j]);
				}
				System.out.printf("\n");
			}
			
			
			
		}
		void setInitialState(String State)
		{
			this.InitialState=new MC_State();
			InitialState.setStateName(State);
			this.States.put(State,InitialState);
		}
		String getInitialStateName()
		{
			return this.InitialState.getStateName();
		}
}

