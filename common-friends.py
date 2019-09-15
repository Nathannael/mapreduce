from mrjob.job import MRJob

class CommonFriends(MRJob):
    def mapper(self, key, line):
        values = line.strip().split(' ')        

        person = values[0]
        friends = values[1:]

        for friend in friends:
            pair = self.build_sorted_key(person, friend)    

            yield pair, friends

    def reducer(self, key, values):
        friends = []

        for item in values:
            friends = friends + item

        yield key, friends

    def build_sorted_key(self, person1, person2):
        if person1 > person2:
            return (person1, person2)
        else:
            return (person2, person1)

if __name__ == '__main__':
    CommonFriends.run()