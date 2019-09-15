from mrjob.job import MRJob
from mrjob.step import MRStep
from dateutil.parser import parse

class MovingAverageProducts(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.group_by_date),
            MRStep(reducer=self.reducer)
        ]

    def mapper(self, _, line):
        data = line.split(',')
        product = data[1].strip()
        country = data[7].strip()
        date = data[0].split()[0].strip()
        price = data[2].strip()
        yield (country, product, str(parse(date)).split()[0]), float(price)

    def group_by_date(self, key, values):
        yield (key[0], key[1]), (key[2], sum(values))

    def reducer(self, key, values):
        self.windowSize = 3
        data = list(values)
        data.sort()

        sum = 0.0
        output_value = None
        for i in range(0, len(data)):
            value_day = data[i][1]
            sum += value_day
            if i < self.windowSize:
                moving_average = sum / (i + 1)
            else:
                sum -= data[i - self.windowSize][1]
                moving_average = sum / self.windowSize
            timestamp = data[i][0]
            output_value = str(timestamp) + ' - ' + str(value_day) + ' - ' + str(round(moving_average, 2))

            key = (key[0].ljust(20), key[1])

            yield key, output_value

if __name__ == '__main__':
    MovingAverageProducts.run()
