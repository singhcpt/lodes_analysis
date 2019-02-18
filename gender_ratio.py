"""
The purpose of this file is to find the male to female ratio of all residents in Chester County 
using 2015 LODES Census data. This script only uses one map reduce step to do this. The purpose 
of this exercise is to familiarize ourselves with the LODES data format as well as check the 
CSV file for any imperfections in the data. 
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class gender_ratio(MRJob):
	
	def steps(self):
		return [
			MRStep(mapper=self.gender_mapper,
				reducer=self.gender_reducer)
		]

	def gender_mapper(self, key, line):
		cols = line.split(',')

		# Checking if variables are missing
		if (len(cols) <= 40):
    			return
		geocode = cols[0]
		# Checking for invalid geocodes
		if (len(geocode) < 15):
    			return
		county = geocode[0:5] # Stripping county codes only from the geocode variable format
		numMale = cols[40]
		numFemale = cols[41]
		# Checking for invalid counts in columns
		if (numMale == "") or (numFemale == ""):
    			return
		if county == "42029":
			 yield county, (numMale, numFemale)

	def gender_reducer(self, key, values):
                femaleCount = 0
                maleCount = 0
                for numMale, numFemale in values:
                        femaleCount += int(numFemale)
                        maleCount += int(numMale)
                yield 'Male to Female Ratio:', (maleCount, femaleCount)

if __name__ == '__main__':
	gender_ratio.run()
