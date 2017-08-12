/*	
 * Copyright (C) 2014 Computer Architecture and Parallel Systems Laboratory (CAPSL)	
 *
 * Original author: Sergio Pino	
 * E-Mail: sergiop@udel.edu
 *
 * License
 * 	
 * Redistribution of this code is allowed only after an explicit permission is
 * given by the original author or CAPSL and this license should be included in
 * all files, either existing or new ones. Modifying the code is allowed, but
 * the original author and/or CAPSL must be notified about these modifications.
 * The original author and/or CAPSL is also allowed to use these modifications
 * and publicly report results that include them. Appropriate acknowledgments
 * to everyone who made the modifications will be added in this case.
 *
 * Warranty	
 *
 * THIS CODE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTY OF ANY KIND,
 * EITHER EXPRESSED OR IMPLIED, INCLUDING, WITHOUT LIMITATION, WARRANTIES THAT
 * THE COVERED CODE IS FREE OF DEFECTS, MERCHANTABLE, FIT FOR A PARTICULAR
 * PURPOSE OR NON-INFRINGING. THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE
 * OF THE COVERED CODE IS WITH YOU. SHOULD ANY COVERED CODE PROVE DEFECTIVE IN
 * ANY RESPECT, YOU (NOT THE INITIAL DEVELOPER OR ANY OTHER CONTRIBUTOR) ASSUME
 * THE COST OF ANY NECESSARY SERVICING, REPAIR OR CORRECTION. THIS DISCLAIMER
 * OF WARRANTY CONSTITUTES AN ESSENTIAL PART OF THIS LICENSE. NO USE OF ANY
 * COVERED CODE IS AUTHORIZED HEREUNDER EXCEPT UNDER THIS DISCLAIMER.
 */

package edu.capsl.fdp.training;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

/**
 * Builds up the estimation of the transition matrix from the sampled transitions.
 * 
 * Loosely based on code from project https://github.com/pranab/avenir
 */
public class MarkovChainModelRed extends Reducer<TransitionWritable, IntWritable, NullWritable, Text> {
	
	private final Log LOG = LogFactory.getLog(MarkovChainModelRed.class);
	
	private String[] states;
	private HashMap<String, TransitionMatrix > trans_matrices;
	
	private NullWritable out_key = NullWritable.get();
	private Text out_value = new Text();
	
	@Override
	protected void setup(Reducer<TransitionWritable, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
    	states = conf.get("mc.states").split(",");
    	
    	LOG.info("Working with  " + states.length + " states : " + Arrays.toString(states));
    	
    	trans_matrices= new HashMap<String, TransitionMatrix>();
    	//trans_matrix = new TransitionMatrix(states);		
	}
	
	@Override
	protected void cleanup(Reducer<TransitionWritable, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		// first record is the states
		out_value.set(context.getConfiguration().get("mc.states"));
		context.write(out_key, out_value);
		
		for (Map.Entry<String, TransitionMatrix> entry :  trans_matrices.entrySet())
		{
			out_value.set("Matrix for Costumer: "+entry.getKey());
			context.write(out_key,out_value);
			TransitionMatrix trans_matrix = entry.getValue();
			// state transitions
			trans_matrix.normalizeRows();
			for (int i = 0; i < states.length; i++) {
				String val = trans_matrix.serializeRow(i);
				out_value.set(val);
				context.write(out_key, out_value);
			}
			out_value.set("------------\n");
			context.write(out_key, out_value);
		}
	}
	
	@Override
	protected void reduce(TransitionWritable key, Iterable<IntWritable> values,
			Reducer<TransitionWritable, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		for (IntWritable val : values)
			sum += val.get();
		
		//LOG.info(key + " " + sum);
		
		
		if (!trans_matrices.containsKey((key.getcID()).toString()))
		{
			TransitionMatrix newTran=new TransitionMatrix(this.states);
			trans_matrices.put((key.getcID()).toString(), newTran);
			LOG.info("Creating Matrix : "+key.getcID().toString());
		}
		
		// adding the sum to the current count on the transition matrix
		trans_matrices.get((key.getcID()).toString()).addTo(key.getPresent(), key.getFuture(), sum);
	}
}