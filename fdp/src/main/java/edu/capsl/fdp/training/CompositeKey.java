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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Used for doing secondary sort. 
 *
 * This code is based on the secondary sort discussed in the book:
 * Owens, Jonathan R., Brian Femiano, and Jon Lentz. Hadoop Real World Solutions Cookbook. Packt Publishing Ltd, 2013.
 */
public class CompositeKey implements WritableComparable<CompositeKey>{

	private final Log LOG = LogFactory.getLog(CompositeKey.class);
	
	private Text cID = null;
	private LongWritable tID = null; 
	
	public CompositeKey(Text customerID, LongWritable transactionID) {
		cID = customerID;
		tID = transactionID;
	}
	
	public CompositeKey() {
		cID = new Text();
		tID = new LongWritable();
	}
	
	public void setcID(String cID) {
		if (this.cID != null)
			this.cID.set(cID);
		else
			LOG.error("the cID is null");
	}
	
	public void settID(long tID) {
		if (this.tID != null)
			this.tID.set(tID);
		else
			LOG.error("the tID is null");
	}
	
	public Text getcID() {
		return cID;
	}
	
	public LongWritable gettID() {
		return tID;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		if (cID == null)
			cID = new Text();
		if (tID == null)
			tID = new LongWritable();
		
		cID.readFields(in);
		tID.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (cID == null || tID == null){
			//TODO do something
		}
		
		cID.write(out);
		tID.write(out);
	}

	@Override
	public int compareTo(CompositeKey other) {
		int cmp = cID.compareTo(other.cID);
		// they are different customers. Then, sort by customer
		if (cmp != 0)
			return cmp;
		// if they are from the same customer, then sort by transaction ID
		return tID.compareTo(other.tID);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (!(obj instanceof CompositeKey))
			return false;
		
		CompositeKey other = (CompositeKey) obj;
		
		return cID.equals(other.cID);
	}
	
	@Override
	public int hashCode() {
		// if you override the equals method is always a good practice to override hashCode
		return cID.hashCode();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("cID: ");
		sb.append(cID.toString());
		sb.append(", tID: ");
		sb.append(tID.get());
		
		return sb.toString();
	}
	
	/**
	 * Uses the customerID information to partition the KVs to the reducers.
	 */
	static class CompositeKeyPartitioner extends Partitioner<CompositeKey, Text> {

		@Override
		public int getPartition(CompositeKey key, Text value, int numPartitions) {
			
			return (key.hashCode() % numPartitions);
		}
		
	}
	
	/**
	 * Groups values together according to the customer ID. Without this component, 
	 * each K2={customerID, transactionID} and its associated V2=transactionType may go to different reducers. 
	 */
	static class CompositeKeyGroupComparator extends WritableComparator {
		
		public CompositeKeyGroupComparator() {
			super(CompositeKey.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			CompositeKey lhs = (CompositeKey) a;
			CompositeKey rhs = (CompositeKey) b;
			
			return lhs.getcID().compareTo(rhs.getcID());
		}
	}
	
	/**
	 * sort the values passed to the reducer.
	 */
	static class CompositeKeySortComparator extends WritableComparator {
		
		public CompositeKeySortComparator() {
			super(CompositeKey.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			CompositeKey lhs = (CompositeKey) a;
			CompositeKey rhs = (CompositeKey) b;
			
			// sorts by customerID
			int cmp = lhs.getcID().compareTo(rhs.getcID());
			if (cmp != 0)
				return cmp;
			
			// Since it is the same customer then sorts by transactionID (secondary Sort)
			return lhs.gettID().compareTo(rhs.gettID());
		}
	}
}
