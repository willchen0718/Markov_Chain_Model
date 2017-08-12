#/usr/bin/env python
# -*- coding: utf -*-

#	
# Copyright (C) 2014 Computer Architecture and Parallel Systems Laboratory (CAPSL)	
#
# Original author: Tu Hao	
# E-Mail: tuhao@udel.edu
# 
# Some improvements
# author: Sergio Pino
# E-mail: sergiop@udel.edu
# 
# License
# 	
# Redistribution of this code is allowed only after an explicit permission is
# given by the original author or CAPSL and this license should be included in
# all files, either existing or new ones. Modifying the code is allowed, but
# the original author and/or CAPSL must be notified about these modifications.
# The original author and/or CAPSL is also allowed to use these modifications
# and publicly report results that include them. Appropriate acknowledgments
# to everyone who made the modifications will be added in this case.
#
# Warranty	
#
# THIS CODE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING, WITHOUT LIMITATION, WARRANTIES THAT
# THE COVERED CODE IS FREE OF DEFECTS, MERCHANTABLE, FIT FOR A PARTICULAR
# PURPOSE OR NON-INFRINGING. THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE
# OF THE COVERED CODE IS WITH YOU. SHOULD ANY COVERED CODE PROVE DEFECTIVE IN
# ANY RESPECT, YOU (NOT THE INITIAL DEVELOPER OR ANY OTHER CONTRIBUTOR) ASSUME
# THE COST OF ANY NECESSARY SERVICING, REPAIR OR CORRECTION. THIS DISCLAIMER
# OF WARRANTY CONSTITUTES AN ESSENTIAL PART OF THIS LICENSE. NO USE OF ANY
# COVERED CODE IS AUTHORIZED HEREUNDER EXCEPT UNDER THIS DISCLAIMER.
#

import string
import random
import numpy as np
import sys

STATE_NUM = 18

def id_generator(length=10, chars=(string.ascii_uppercase + string.digits)):
	return ''.join(random.choice(chars) for _ in range(length))

def customer_id_list_generator(size=100):
	'''
	generate a list with #size customer_ids
	'''
	customer_ids = []
	for i in range(size):
		cid = id_generator()
		print i
		customer_ids.append(cid)
	return customer_ids

def customer_matrix_generator():
	'''
	generate a random transition matrix for customer_id
	'''
	# random a 18*18 matrix
	mat = np.random.randint(100, size=(STATE_NUM, STATE_NUM))
	# add 1 to every item to ensure no item equal zero
	mat = mat + np.ones((STATE_NUM, STATE_NUM))
	# get sum of each row
	row_sum = [0.0]*STATE_NUM
	for row in range(STATE_NUM):
		s_row = sum(mat[row])
		row_sum[row] = s_row
	
	# each item in a row divide sum
	for row in range(STATE_NUM):
		for col in range(STATE_NUM):
			mat[row][col] = mat[row][col] / row_sum[row]
	
	# checks
	for row in range(STATE_NUM):
		s_row = sum(mat[row])
		if abs(s_row - 1.0) > 0.05:
			print "ERROR: For row " + str(row) + " the sum was " + str(s_row)
			sys.exit(-1)
	
	return mat

if __name__ == "__main__":
	PREFIX = 'customers/'
	customer_ids = customer_id_list_generator(1000)
	for cid in customer_ids:
		filename = PREFIX + cid
		# save the matrix to a file named with customer_id
		np.save(filename, customer_matrix_generator())
