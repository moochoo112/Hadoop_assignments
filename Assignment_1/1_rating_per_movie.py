
from mrjob.job import MRJob
from mrjob.step import MRStep

AMOUNT_OF_LEADING_ZEROS_FOR_RATINGS = 5 # the amount of leading zeros used in the rating count to sort correctly

class RatingsBreakdown(MRJob):

    # mrjobs steps
    def steps(self):
        return [
            # chaining the steps together, the second one takes the output of the first one
            MRStep( mapper=self.mapper_get_movies, # we map the output from the file received to get the movie id's with as many ratings they have
                reducer=self.reducer_count_ratings), # we apply the first reducer to sum all the intances in which a movie was rated
            MRStep( reducer=self.reducer_sort_output) # we reduce the result sorted and print it
        ]

    # a function to map the u.data input received, the result should be: [key: movieID, value: 1]
    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t') # we separate the rows by tabs  in between each other
        yield movieID, 1 # we select and pass the movieId as the key, and it will receive 1 as a value
    
    # a function to reduce the result that count the instances in which a movieID received a rating
    def reducer_count_ratings(self, movieID, values):
        yield str(sum(values)).zfill(AMOUNT_OF_LEADING_ZEROS_FOR_RATINGS), movieID # sum the values that share the movieId turn the value into a string 
                                                                              # and fill the numbers with 0s to the left so it can be sorted, 
                                                                              # then pass the movieId 
    
    # a function to print as a last step the movie id with the count of ratings sorted
    def reducer_sort_output(self, ratingsCount, movies):
        for movie in movies: # loop through the movieIds received in the previous reducer
            yield 'Movie ' + movie, ratingsCount + ' ratings.' # output each movie with the count of ratings ascendingly

if __name__ == '__main__':
    RatingsBreakdown.run()