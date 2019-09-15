from mrjob.job import MRJob
from mrjob.step import MRStep

class QuantityPerCountry(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.grouper)
        ]

    def mapper(self, _, line):
        data = line.split(',')
        product = data[1].strip()
        country = data[7].strip()
        yield (country, product), 1

    def reducer(self, key, values):
        qtd = sum(values)
        yield key[0], (key[1], qtd)

    def grouper(self, country, groups):
        yield country, list(groups)

if __name__ == '__main__':
    QuantityPerCountry.run()
