from mrjob.job import MRJob

class MaxTemperature(MRJob):
    def mapper(self, _, line):
        temp_info = line.split(',')
        location = temp_info[0]
        measure = temp_info[2]
        value = temp_info[3]
        if measure in ['TMIN', 'TMAX']:
            yield location, { 'measure': measure, 'temperature': float(value) }  

    def reducer(self, location, dictionaries):
        t_min = 0
        t_max = 0
        for value in dictionaries:
           if value['measure'] == 'TMIN':
               if t_min > value['temperature']:
                t_min = value['temperature']
           if value['measure'] == 'TMAX':
               if t_max < value['temperature']:
                   t_max = value['temperature']
        yield location, {'max': t_max, 'min': t_min}

if __name__ == '__main__':
    MaxTemperature.run()
