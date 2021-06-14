from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsCalculator(MRJob):

    def steps(self):
        return [
            # chaining the steps together, the second one takes the output of the first one
            # we map the output from the file received to get the movie id's with as many ratings they have
            # we apply the first reducer to sum all the intances in which a movie was rated
            # we reduce the result sorted and print it
            MRStep(
                mapper=self.mapper_get_ratings, 
                combiner=self.combiner_count_ratings,
                reducer=self.reducer_count_ratings            
            )
            ,
            MRStep(
                reducer=self.reducer_sort_counts
            )
            
        ]
	
    # a function to map the u.data input received,	
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t') # we separate the rows by tabs  in between each other
        yield movieID, 1 # we select and pass the movieId as the key, and it will receive 1 as a value

    # Combiner: aggregate (sum up) some of the values (ratings) for the same keys (movieID). 
    # The amount of rows will be strongly reduced 
    def combiner_count_ratings(self, movieID, counts):
        yield movieID, sum(counts)

    # Reducer: The counted ratings need to become the new key value, because they need to be sorted. 
    # Because we want to sort on the amount of counted ratings, we need this as a new key. 
    # A new key/value pair needs to be made: countedRatings/movieID and is returned as the value.
    # We do now have new pairs like (None, (countedRatings, movieID)). This’ll serve as the input for the sorting in the next step. 
    # Sum is used to get the total ratings per movieID.
    def reducer_count_ratings(self, movieID, counts):
        yield None, (sum(counts), movieID)

    # We’re looping through the sorted rows to yield the highest count first and the lowest one last. 
    # Reverse=True is needed in the ‘sorted’ function, otherwise the movieID with the greatest amount of rating-counts would be last. 
    # The movieID in the yield is transformed to an integer for readability.   
    def reducer_sort_counts(self, _, values):
        for counted_ratings, movieID in sorted(values, reverse=True):
            yield (counted_ratings, int(movieID))


if __name__ == '__main__':
    RatingsCalculator.run()