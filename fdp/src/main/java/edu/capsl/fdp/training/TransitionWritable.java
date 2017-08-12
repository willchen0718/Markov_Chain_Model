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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Represents a sampled transition from a sequence of history data.
 */
public class TransitionWritable implements WritableComparable<TransitionWritable> {

	private final Log LOG = LogFactory.getLog(TransitionWritable.class);
	
	private Text cID = null;
	private String present;
	private String future;
	
	public TransitionWritable(){
		this.cID=new Text();
	}
	
	public void setPresent(String present) {
		this.present = present;
	}
	
	public void setFuture(String future) {
		this.future = future;
	}
	
	public void setcID(String cID) {
		if (this.cID != null)
			this.cID.set(cID);
		else 
			LOG.error("the cID is null");
	}
		
	public String getPresent() {
		return present;
	}
	
	public String getFuture() {
		return future;
	}
	
	public Text getcID() {
		return cID;
	}
		
	
	@Override
	public void readFields(DataInput in) throws IOException {
		if (cID == null)
			cID = new Text();
				
		cID.readFields(in);
		present = in.readUTF();
		future = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (cID == null ||present == null || future == null) {
			//LOG.warn("either cID, present or future was null!");
			return;
		}
		cID.write(out);
		out.writeUTF(present);
		out.writeUTF(future);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(cID.toString()+" ");
		sb.append(present);
		sb.append(" ");
		sb.append(future);
		
		return sb.toString();
	}

	@Override
	public int compareTo(TransitionWritable other) {
		
		int cmp = cID.compareTo(other.cID);
		if (cmp != 0)
			return cmp;
		
		cmp = present.compareTo(other.present);
		if (cmp != 0)
			return cmp;
		
		cmp = future.compareTo(other.future);
		return cmp;
	}
	
	@Override
	public int hashCode() {
		
		return (cID.toString()+present+future).hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (!(obj instanceof TransitionWritable))
			return false;
		
		TransitionWritable other = (TransitionWritable) obj;
		
		return cID.equals(other.cID) & present.equals(other.present) & future.equals(other.future);
	}
}
