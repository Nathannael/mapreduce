from mrjob.job import MRJob

class TotalOrderAmount(MRJob):
    def mapper(self, _, line):
        _, _, age, number_of_friends = line.split(',')
        yield age, int(number_of_friends)

    def reducer(self, age, friends_numbers):
        count = 0
        total = 0
        for number in friends_numbers:
            count += 1
            total += number
        yield age, total/count

if __name__ == '__main__':
    TotalOrderAmount.run()