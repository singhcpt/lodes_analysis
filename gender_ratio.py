from mrjob.job import MRJob
from mrjob.step import MRStep
import re
#
class gender_ratio(MRJob):
	
	def steps(self):
		return [
			MRStep(mapper=self.gender_mapper,
				reducer=self.gender_reducer)
		]

	def gender_mapper(self, key, line):
		cols = line.split(',')
		geocode = cols[0]
		county = geocode[0:5]
		print (geocode)
		numMale = cols[40]
		numFemale = cols[41]
		if county == "42029":
			 yield county, (numMale, numFemale)

	def gender_reducer(self, key, values):
                femaleCount = 0
                maleCount = 0
                for numMale, numFemale in values:
                        femaleCount += numFemale
                        maleCount += numMale
                yield 'Sum', (maleCount, femaleCount)

if __name__ == '__main__':
	gender_ratio.run()
