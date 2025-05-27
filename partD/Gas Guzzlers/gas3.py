from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time
import os
import json

class gas3(MRJob):

	top2 = ['0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444', '0xfa52274dd61e1643d2205169732f29114bc240b3']

	
	def mapper1(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 7:
				toAddress = str(fields[2])
				if toAaddress in self.top2:
					block = float(fields[0])
					gasPrice = float(fields[5])
					timestamp = int(fields[6])
					month = time.strftime("%m", time.gmtime(timestamp))
					year = time.strftime("%y", time.gmtime(timestamp))
					timeKey = (year, month)
					value = (0, gasPrice)
					yield ((block, timeKey), value)

			elif len(fields) == 9:
				block = float(fields[0])
				difficulty = float(fields[3])
				timestamp = int(fields[7])
				month = time.strftime("%m", time.gmtime(timestamp))
				year = time.strftime("%y", time.gmtime(timestamp))
				timeKey = (year, month)
				value = (1, difficulty)
				yield ((block, time_key), value)


		except:
			pass

	def reducer1(self,word,counts):

		diffList = []
		gasList = []
		flag = False

		for each in counts:

			if each[0]==0 :
				gasList.append(each[1])
				flag = True
			elif each[0]==1:
				diffList.append(each[1])			

		if flag and gasList and diffList:
			for gas, diff in zip(gasList, diffList):
				yield (word[1],(gas, diff))

	def mapper2(self, key,values):
		yield (key, (values[0], values[1], 1))

	def reducer2(self,word,counts):
		
		count = 0
		totalDifficulty = 0
		totalGas = 0
		for each in counts :
				
			totalGas += each[0]	
			totalDifficulty += each[1]	
			count += each[2]
							

		averageDifficulty = float(totalDifficulty)/ float(count)
		averageGas = float(totalGas)/ float(count)

		yield(word,(averageGas, averageDifficulty))

		

	def combiner2(self,word,counts):

		count = 0
		totalDifficulty = 0
		totalGas = 0
		for each in counts :
				
			totalGas += each[0]	
			totalDifficulty += each[1]	
			count += each[2]
							
		yield(word,(totalGas, totalDifficulty, count))
		


	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, combiner = self.combiner2, reducer = self.reducer2)]


if __name__ == '__main__':

	gas3.run()
