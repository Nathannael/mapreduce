from mrjob.job import MRJob
from mrjob.step import MRStep

class BestMovies(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.flatten_movies)
        ]

    def mapper(self, _, line):
        _, movie_id, rating, _ = line.split()
        yield movie_id, float(rating)  

    def reducer(self, movie_id, movie_ratings):
        ratings = list(movie_ratings)
        avg = sum(ratings)/len(ratings)

        yield None, {'movie_id': movie_id, 'avg': avg}
    
    def flatten_movies(self, _, movies_with_avg):
        list_of_movies = list(movies_with_avg)
        sorted_by_avg = sorted(list_of_movies, key=lambda i: i['avg'])[-10:]
        for movie_with_avg in sorted_by_avg:
            yield movie_with_avg['movie_id'], movie_with_avg['avg']

if __name__ == '__main__':
    BestMovies.run()
