from mrjob.job import MRJob

class MRSalaries(MRJob):
    sala
    def mapper(self, _, line):
        (name,jobTitle,agencyID,agency,hireDate,annualSalary,grossPay) = line.split('\t')
        pay = float(annualSalary)
        if pay >= 0.00 and pay <= 49999.99 :
            yield "Low", 1
        if pay >= 50000.00 and pay <= 99999.99 :
            yield "Medium", 1
        if pay >= 100000.00 :
            yield "High", 1

    def combiner(self, pay, counts):
        yield pay, sum(counts)

    def reducer(self, pay, counts):
        yield pay, sum(counts)


if __name__ == '__main__':
    MRSalaries.run()


