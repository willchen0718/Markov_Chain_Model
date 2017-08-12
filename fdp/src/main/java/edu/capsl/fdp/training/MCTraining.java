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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MCTraining {
	
	static String USAGE = "hadoop jar PATH_TO_FDP_JAR /input/data/path(can be an hdfs folder) /output/path(has to be an existing folder)";
	
	/**
	 * Returns the job that performers the sequence creation from the transaction input data
	 * @param input String that represents the path in HDFS that has the input dataset
	 * @param working_path Path in HDFS to store the temporal/output data
	 * @return Job instance
	 * @throws IOException
	 */
	public static Job getSequenceBuilderJob(String input, String working_path) throws IOException {
		Configuration conf = new Configuration();
	    
		
	    Job job = new Job(conf);
	    job.setJobName("SequenceBuilder");
		job.setJarByClass(MCTraining.class);
		
	    // mapper configuration
	    job.setMapperClass(SequenceBuilderMap.class);
	    job.setMapOutputKeyClass(CompositeKey.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    // intermediate
	    job.setPartitionerClass(CompositeKey.CompositeKeyPartitioner.class);
	    job.setGroupingComparatorClass(CompositeKey.CompositeKeyGroupComparator.class);
	    job.setSortComparatorClass(CompositeKey.CompositeKeySortComparator.class);
	    
	    // reducer configuration
	    job.setReducerClass(SequenceBuilderRed.class);
	    job.setOutputKeyClass(TransitionWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    
	    String tmp_out = (working_path.charAt(working_path.length()-1) == '/' ? "seq_out" : "/seq_out");
	    FileOutputFormat.setOutputPath(job, new Path(working_path + tmp_out));
	    
	    return job;
	}
	
	/**
	 * Returns the job that performers the Markov chain training
	 * 
	 * @param working_path Path in HDFS to store the temporal/output data
	 * @param states String with the states to be used in the training. It is suppose you know this s before training.
	 * @return Job instance
	 * @throws IOException
	 */
	public static Job getMarkovChainTrainingJob(String working_path, String states) throws IOException {
		
		Configuration conf = new Configuration();
	    		
		conf.set("mc.states", states);
		
	    Job job = new Job(conf);
	    job.setJobName("Markov chain model training");
		job.setJarByClass(MCTraining.class);
		
	    // mapper configuration
	    job.setMapperClass(MarkovChainModelMap.class);
	    job.setMapOutputKeyClass(TransitionWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    // reducer configuration
	    job.setReducerClass(MarkovChainModelRed.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    String seq_out = (working_path.charAt(working_path.length()-1) == '/' ? "seq_out" : "/seq_out");
	    FileInputFormat.addInputPath(job, new Path(working_path + seq_out));
	    
	    String out = (working_path.charAt(working_path.length()-1) == '/' ? "mc_out" : "/mc_out");
	    FileOutputFormat.setOutputPath(job, new Path(working_path + out));
	    
	    return job;
	}
	
	public static void main(String[] args) throws IllegalArgumentException, Exception {
		
		if (args.length != 2) {
			System.err.println(USAGE);
			System.exit(-1);
		}
		
		String input_path = args[0];
		String working_path = args[1];
		String states = "LNL,MNL,HNL,"
				+ "LHL,MHL,HHL,"
				+ "LNN,MNN,HNN,"
				+ "LHN,MHN,HHN,"
				+ "LNS,MNS,HNS,"
				+ "LHS,MHS,HHS";
		
		
		System.out.println("Working with input " + input_path + " and writing output data to " + working_path);
		
		// builds the "sequence" from the input data (not really)
		Job seq_builder = getSequenceBuilderJob(input_path, working_path);
	    if (!seq_builder.waitForCompletion(true))
	    	System.exit(-1);

	    // markov chain training
	    Job markov_trainer = getMarkovChainTrainingJob(working_path, states);
	    System.exit(markov_trainer.waitForCompletion(true) ? 0 : 1);
	}
}
