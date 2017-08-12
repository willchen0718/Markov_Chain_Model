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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Just an reconstruct the transition information from the HDFS file of the previous job. 
 * Kind of identity map
 */
public class MarkovChainModelMap extends Mapper<Object, Text, TransitionWritable, IntWritable> {

	private final Log LOG = LogFactory.getLog(MarkovChainModelMap.class);
	
	private TransitionWritable out_key = new TransitionWritable();
	private IntWritable out_value = new IntWritable();
	
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, TransitionWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		// split by space or tab
		String[] tokens = value.toString().split("[[ ]*\t]");
		
		if (tokens.length == 4) {
			
			// TODO the trim is probably not needed
			out_key.setcID(tokens[0].trim());
			out_key.setPresent(tokens[1].trim());
			out_key.setFuture(tokens[2].trim());
			
			out_value.set(Integer.parseInt(tokens[3]));
						
			context.write(out_key, out_value);
			
			//LOG.info("'" + out_key + "' : " + out_value);
			
		} else
			LOG.warn("The number of tokens is not 3, instead is " + tokens.length + " for the string " + value);
		
	}
}
