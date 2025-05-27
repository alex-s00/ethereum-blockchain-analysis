from mrjob.job import MRJob
import time

class partA2(MRJob):

	def mapper(self,_, line):
		fields = line.split(',')
		try:
			if len(fields)==7:
				blockTimestamp = int(fields[6])
				value = float(fields[3])
				month = time.strftime("%m", time.gmtime(blockTimestamp))
				year = time.strftime("%y", time.gmtime(blockTimestamp))
				yield((month,year),value)
		except:
			pass
	def combiner(self, date, price):
		sum = 0
		count = 0
		for p in price:
			sum += p 
			count = count +1
		yield(date, (sum, count))

	def reducer(self,date,price):     
		sum = 0
		count = 0
		for p in price:
			sum+=p[0]
			count+=p[1]
		average = sum/count
		yield(date,average)


if __name__ == '__main__':
	partA2.run()
