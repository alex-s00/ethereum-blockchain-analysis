from mrjob.job import MRJob
import time

class ethereumFork(MRJob):
    def mapper(self, _ , lines):
        try:
            field = lines.split(',')
            if (len(field) == 7):
                blockTimestamp = int(field[6])
                date = time.gmtime(float(field[6]))
                gasPrice = float(field[5])
                day = time.strftime("%d" ,time.gmtime(blockTimestamp))
                month = time.strftime("%m" ,time.gmtime(blockTimestamp))
                year = time.strftime("%y" ,time.gmtime(blockTimestamp))
                if (date.tm_year== 2016 and date.tm_mon== 7):
                    yield((date.tm_mday), (1,gasPrice))

        except:
            pass


    def combiner(self, key, value):
        count = 0
        total = 0
        for v in value:
            count+= v[0]
            total+= v[1]

        yield (key, (count, total))

    def reducer(self, key, value):
        count = 0
        total = 0
        for v in value:
            count+= v[0]
            total+= v[1]

        yield (key, (count, total))


if __name__=='__main__':
    ethereumFork.run()
