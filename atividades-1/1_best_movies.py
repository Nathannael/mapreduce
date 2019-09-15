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
        _, movie_id, rating, _ = line.split()
        yield movie_id, float(rating)  

    def reducer(self, movie_id, movie_ratings):
        ratings = list(movie_ratings)
        avg = sum(ratings)/len(ratings)

        yield avg, movie_id
    
    def sorter(self, avg, movie_ids):
        for movie_id in movie_ids:
            yield round(avg, 2), movie_id

if __name__ == '__main__':
    BestMovies.run()
