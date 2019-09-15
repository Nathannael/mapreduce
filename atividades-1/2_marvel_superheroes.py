from mrjob.job import MRJob
from mrjob.step import MRStep


class BestMovies(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.sorter)
        ]

    def mapper(self, _, line):
        superhero, *friends = line.split()
        yield superhero, len(friends)

    def reducer(self, superhero, num_friends):
        yield "1", {'superhero': superhero, 'num_friends': sum(num_friends)}

    def sorter(self, _, hero_with_friends):
        response = {'superhero': '0', 'num_friends': 0}

        for hero in hero_with_friends:
            if hero['num_friends'] > response['num_friends']:
                response = hero

        yield response['superhero'], response['num_friends']

if __name__ == '__main__':
    BestMovies.run()
