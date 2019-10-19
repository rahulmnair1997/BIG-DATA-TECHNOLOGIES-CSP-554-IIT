from mrjob.job import MRJob

class MRMovieCount(MRJob):

    def mapper(self, _, line):
        (USERID,MOVIEID,RATING,TIMESTAMP) = line.split(',')
        yield USERID, 1

    def combiner(self, USERID, counts):
        yield USERID, sum(counts)

    def reducer(self, USERID, counts):
        yield USERID, sum(counts)


if __name__ == '__main__':
    MRMovieCount.run()


