from mrjob.job import MRJob
from mrjob.step import MRStep

class MaxMinTemperature(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.location_reducer)
        ]

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
    
    def location_reducer(self, location, min_max):
        min_max = list(min_max)
        yield location, {'min': min_max[1][1], 'max': min_max[0][1]}

if __name__ == '__main__':
    MaxMinTemperature.run()
