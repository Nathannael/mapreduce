from mrjob.job import MRJob
from mrjob.step import MRStep


class MovingAverage(MRJob):
    def mapper(self, _, line):
        name, timestamp, value = line.split(',')
        yield name, (timestamp, float(value))

    def reducer(self, key, values):
        self.window_size = 2
        data = list(values)
        data.sort()

        sum = 0.0
        output_value = None
        for i in range(0,len(data)):
            sum += data[i][1]
            if i < self.window_size:
                moving_average = sum / (i+1)
            else:
                sum -= data[i - self.window_size][1]
                moving_average = sum / self.window_size
            timestamp = data[i][0]
            output_value = str(timestamp) + ', ' + str(round(moving_average, 2))

            yield key, output_value

if __name__ == '__main__':
    MovingAverage.run()
