from mrjob.job import MRJob

class MaxTemperature(MRJob):
    def mapper(self, _, line):
        temp_info = line.split(',')
        _location = temp_info[0]
        _measure = temp_info[2]
        _value = temp_info[3]
        yield int(temp_info)

    def reducer(self, age, friends_numbers):
        count = 0
        total = 0
        for number in friends_numbers:
            count += 1
            total += number
        yield age, total/count

if __name__ == '__main__':
    MaxTemperature.run()