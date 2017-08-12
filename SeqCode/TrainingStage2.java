

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.HashMap;
import java.util.TreeMap;

//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.util.*;


public class TrainingStage2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		HashMap<String,TreeMap < String , String > > allTheStates=new HashMap<String,TreeMap<String,String> >();

		HashMap<String, MarkovChain> MCs=new HashMap<String,MarkovChain>();
		//MarkovChain MC=new MarkovChain();
		String[] files = {"Sequence.txt","Sequence2.txt","Sequence3.txt"};
		for(String file : files)
		{
			try 
			{
				
				BufferedReader br = new BufferedReader(new FileReader(file));
			        //StringBuilder sb = new StringBuilder();
			        String line = br.readLine();
	        		String[] tokens;
	
				while (line!=null)
				{
					tokens=line.split(",");
					if(!allTheStates.containsKey(tokens[0]))
					{
						TreeMap<String,String> newTreeMap=new TreeMap<String,String>();
						newTreeMap.put(tokens[1],tokens[2]);
						allTheStates.put(tokens[0],newTreeMap);
					}
					else
					{
						allTheStates.get(tokens[0]).put(tokens[1],tokens[2]);
					}

				line=br.readLine();
				}
			}
			

			catch (FileNotFoundException fnf)
	    		{
	    			fnf.printStackTrace();
      	        	} 
			catch (IOException e)
	        	{
	    			System.out.println(e.toString());
	    		}
		}


	        //We expect the first line to have the initial state.
	        //MC.setInitialState(String.valueOf(line.charAt(0)));
	        //line = br.readLine();
	        //HashMap<String, String> previousState = new HashMap<String,String>();
	        String previousState= new String();
	        //String previousCostumer = new String();
	        for( Map.Entry<String,TreeMap<String,String> > entry : allTheStates.entrySet())
		{
			for(Map.Entry <String,String> TreeEntry : entry.getValue().entrySet())
			{
		        	if(!MCs.containsKey(entry.getKey()))
		        	{
	        			MarkovChain MC = new MarkovChain();
	        			MC.setInitialState(TreeEntry.getValue());
	        			MCs.put(entry.getKey(),MC);
					previousState=TreeEntry.getValue();
		        	}
	        		else
	        		{
	        			MCs.get(entry.getKey()).insertOrIncrementState(TreeEntry.getValue());
		        		MCs.get(entry.getKey()).IncrementTransition(previousState,TreeEntry.getValue());
					previousState=TreeEntry.getValue();

	        		}
	        }
	    }
		for (Map.Entry <String, MarkovChain> MC : MCs.entrySet())
			{
				MC.getValue().PrintTransitionMatrix();
			}
	}

}
