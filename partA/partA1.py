from mrjob.job import MRJob
import time

class partA1(MRJob):
	
	def mapper(self, _,line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				blockTimestamp = int(fields[6])
				month = time.strftime("%m", time.gmtime(blockTimestamp))
				year = time.strftime("%y", time.gmtime(blockTimestamp))
				yield ((month,year), 1)
		except:
			pass

	def reducer(self,word,counts):
		yield(word,sum(counts))

if __name__ == '__main__':
	partA1.run()
