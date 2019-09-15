from mrjob.job import MRJob

class MaxTemperature(MRJob):
    def mapper(self, _, line):
        temp_info = line.split(',')
        location = temp_info[0]
        measure = temp_info[2]
        value = temp_info[3]
        if measure in ['TMIN', 'TMAX']:
            yield (location, measure), float(value)  

    def reducer(self, key, values):
        loc = key[0]
        type = key[1]
        if type == 'TMAX':
            yield loc, (type, max(values))
        if type == 'TMIN':
            yield loc, (type, min(values))

if __name__ == '__main__':
    MaxTemperature.run()
