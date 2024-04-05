from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time
import os
import json

class gas2(MRJob):

	
	def mapper1(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 9:
				blockNo = float(fields[0])
				difficulty = float(fields[3])
				gasUsed = float(fields[6])
				timestamp = int(fields[7])
				month = time.strftime("%m", time.gmtime(timestamp))   
				year = time.strftime("%Y", time.gmtime(timestamp)) 
				timeKey = (year, month)
				value = (0, timeKey, gasUsed, difficulty)
				yield (blockNo, value)

			elif len(fields) == 5:
				address = fields[0]
				blockNo = float(fields[3])
				yield (blockNo, (1,"In"))
		except:
			pass

	def reducer1(self,word,counts):

		valuesList = []
		flag = False

		for each in counts:

			if each[0]==0 :
				valuesList.append((each[1],each[2], each[3]))
			elif each[0]==1:
				flag = True
				

		if flag:
			if valuesList :
				for each in valuesList:
					yield (each[0],(each[1],each[2]))

	def mapper2(self, key,values):
		yield (key, (values[0], values[1], 1))

	def reducer2(self,word,counts):

		count = 0
		totalDif = 0
		totalGas = 0
		for each in counts :
			totalGas += each[0]
			totalDif += each[1]
			count += each[2]

		averageDif = float(totalDif)/ float(count)
		averageGas = float(totalGas)/ float(count)

		yield(word,(averageGas, averageDif))

										


	def combiner2(self,word,counts):
	
		count = 0
		totalDif = 0
		totalGas = 0
		for each in counts :
			totalGas += each[0]
			totalDif += each[1]
			count += each[2]

		intermVals = (totalGas, totalDif, count)

		yield(word,intermVals)

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, combiner = self.combiner2, reducer = self.reducer2)]




if __name__ == '__main__':

	gas2.run()
