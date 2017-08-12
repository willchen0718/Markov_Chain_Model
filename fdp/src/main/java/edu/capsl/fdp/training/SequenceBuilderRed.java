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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Since we used the secondary sort, then we know that the iterable values that enter to the reducer are sorted 
 * by the transactionID. This is true for every customer. Thus, taking advantage of this, the reducer just sample
 * the sequence (the sorted values in the iterable, in this implementation you never reconstruct the sequence explicitly)
 * and outputs to HDFS the transitions that you suppose to be able to build with the sequence.
 */
public class SequenceBuilderRed extends Reducer<CompositeKey, Text, TransitionWritable, IntWritable> {
	
	private final Log LOG = LogFactory.getLog(SequenceBuilderRed.class);
	
	private final IntWritable ONE = new IntWritable(1);
	private TransitionWritable out_key = new TransitionWritable();
	
	@Override
	protected void reduce(CompositeKey key, Iterable<Text> values,
			Reducer<CompositeKey, Text, TransitionWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		int count = 0;
		
		// use the iterator to create the transitions
		if (it.hasNext()) {
			
			String current_state = it.next().toString();
			while (it.hasNext()) {
				String next_state = it.next().toString();
				out_key.setcID(key.getcID().toString());
				out_key.setPresent(current_state);
				out_key.setFuture(next_state);
				
				context.write(out_key, ONE);
				current_state = next_state;
				
				count++;
				LOG.debug("transition " + out_key);
			}
			
		}
		
		LOG.info("Processed Key: " + key.getcID() + " with " + count + " sampled transitions");
	}
}
