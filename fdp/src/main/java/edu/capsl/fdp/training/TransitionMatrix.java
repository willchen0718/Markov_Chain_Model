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

import java.util.Map;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Transition matrix for the markov chain
 * 
 * Loosely based on code from project https://github.com/pranab/avenir and https://github.com/pranab/chombo
 */
public class TransitionMatrix {
	
	private final Log LOG = LogFactory.getLog(TransitionMatrix.class);
	private static final String DELIMETER = ",";
	
	private Map<String, Integer> label_to_pos;
	private double[][] transMatrix;
	private int numStates;
	
	public TransitionMatrix(String[] state_labels) {
		numStates = state_labels.length;
		
		transMatrix = new double[numStates][numStates];
		label_to_pos = new HashMap<String, Integer>();
		
		int pos = 0;
		for (String state : state_labels) {
			label_to_pos.put(state, pos);
			LOG.info("label: " + state + " in pos = " + pos);
			pos++;
		}
		
		LOG.info("creating an " + numStates + "x" + numStates + " transition matrix");
	}

	public void addTo(String present, String future, int val) {
		
		int row = label_to_pos.get(present);
		int col = label_to_pos.get(future);
		
		transMatrix[row][col] += val;
	}
	
	public void normalizeRows() {
		// laplace smoothing function
		for (int r = 0; r < numStates; r++) {
			
			boolean gotZeroCount = false;
			for (int c = 0; c < numStates && !gotZeroCount; c++) {
				gotZeroCount = transMatrix[r][c] == 0;
			}
			
			if (gotZeroCount) {
				LOG.info("row " + r + " has to use the laplace smoothing function");
				for (int c = 0; c < numStates; c++) {
					transMatrix[r][c] += 1;
				}			
			}
		}		
		
		//normalize
		double rowSum = 0;
		for (int r = 0; r < numStates; r++) {
			double check_sum = 0.0;
			rowSum = getRowSum(r);
			for (int c = 0; c < numStates; c++) {
				transMatrix[r][c] = transMatrix[r][c] / rowSum;
				check_sum += transMatrix[r][c];
			}
			LOG.info("row=" + r + " sum=" + check_sum);
		}
	}
	
	public int getRowSum(int row) {
		int sum = 0;
		for (int c = 0; c < numStates; c++) {
			sum += transMatrix[row][c];
		}
		return sum;
	}
	
	public String serializeRow(int row) {
		StringBuilder stBld = new StringBuilder();
		for (int c = 0; c < numStates; c++) {
			stBld.append(transMatrix[row][c]).append(DELIMETER);
		}
		
		return stBld.substring(0, stBld.length() - DELIMETER.length());
	}
	
	public void deseralizeRow(String data, int row) {
		String[] items = data.split(DELIMETER);
		int k = 0;
		for (int c = 0; c < numStates; c++) {
			transMatrix[row][c]  = Double.parseDouble(items[k++]);
		}
	}
	
	
}
