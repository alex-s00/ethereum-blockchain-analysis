import pyspark
import time

startTime = time.time()

def transact(line):
        try:
                fields = line.split(',')
                if len(fields)!=7:
                        return False
                int(fields[3])
                return True

        except:
                return False

def contract(line):
        try:
                fields = line.split(',')
                if len(fields)!=5:
                        return False
                return True
        except:
                return False

sc = pyspark.SparkContext()

transactions = sc.textFile("/data/ethereum/transactions")
validTransactions = transactions.filter(transact)
mapTransactions = validTransactions.map(lambda i : (i.split(',')[2], int(i.split(',')[3])))
aggregateTransactions = mapTransactions.reduceByKey(lambda c,d : c+d)
contracts = sc.textFile("/data/ethereum/contracts")
validContracts = contracts.filter(contract)
mapContracts = validContracts.map(lambda j: (j.split(',')[0], None))
join = aggregateTransactions.join(mapContracts)

top10 = join.takeOrdered(10, key = lambda l: -l[1][0])

endTime = time.time()
totalTime = endTime-startTime

with open('PARTDcompare.txt', 'w') as f:
        for value in top10:
                f.write("{}:{}\n".format(value[0],value[1][0]))
	f.write("\n" + str(totalTime))
