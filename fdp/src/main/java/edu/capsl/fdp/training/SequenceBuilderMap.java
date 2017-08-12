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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to performed secondary sort. The idea is to send to the reducer the key-values partitioned 
 * by customerID and sorted by transactionID.
 * 
 * This code is based on the secondary sort discussed in the book:
 * Owens, Jonathan R., Brian Femiano, and Jon Lentz. Hadoop Real World Solutions Cookbook. Packt Publishing Ltd, 2013.
 */
public class SequenceBuilderMap extends Mapper<LongWritable, Text, CompositeKey, Text> {
	
	private final Log LOG = LogFactory.getLog(SequenceBuilderMap.class);
	private CompositeKey outkey = new CompositeKey();
	private Text trType = new Text();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CompositeKey, Text>.Context context)
			throws IOException, InterruptedException {

		// each record has the format cID, tID, tType
		String[] tokens = value.toString().split(",");
		
		if (tokens.length == 3) {
			String cID = tokens[0].trim();
			long tID = Long.parseLong(tokens[1].trim());
			String tType = tokens[2].trim();
			
			// creating the composite key
			outkey.setcID(cID);
			outkey.settID(tID);
			
			// setting the transaction type
			trType.set(tType);
			
			LOG.debug(outkey + ", tTYPE: " + trType);
			
			context.write(outkey, trType);
			
		} else {
			LOG.warn("The tokens' length is not 3, intead is " + tokens.length);
		}
	}
}
