from mrjob.job import MRJob
import re
import time
import os
import json

class gas1(MRJob):

	
	def mapper(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 7:
				gasPrice = float(fields[5])
				timestamp = int(fields[6])
				month = time.strftime("%m", time.gmtime(timestamp))
				year = time.strftime("%y", time.gmtime(timestamp))
				timeKey = (year, month)
				value = (gasPrice, 1)
				yield (timeKey, value)
		except:
			pass

	def reducer(self,word,counts):

		count = 0
		totalVal = 0
		for value in counts :
			totalVal += value[0]
			count += value[1]

		avgValues = float(totalVal)/ float(count)

		yield(word,avgValues)





if __name__ == '__main__':

	gas1.run()
