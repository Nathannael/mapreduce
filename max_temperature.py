from mrjob.job import MRJob

class MaxTemperature(MRJob):
    def mapper(self, _, line):
        temp_info = line.split(',')
        location = temp_info[0]
        measure = temp_info[2]
        value = temp_info[3]
        if measure == 'TMAX':
            yield location, int(value)  

    def reducer(self, location, values):
        yield location, max(list(values))

if __name__ == '__main__':
    MaxTemperature.run()