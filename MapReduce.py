from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
	def steps(self):
		return [MRStep(mapper=self.mapper_get_ratings, reducer=self.reducer_count_ratings)]
	
	def mapper_get_ratings (self, -, line): #defining a mapper for MapReduce in Python. A mapper requires three input arguments: self is the object the function is instanced in, - usually unused in mapper but it could be a key comming from another reducer, line refers to the input line the function is applied on
		(userID, moveiID, rating, timestamp) = line.split('\t')
		yield rating, 1 #returns only rating and 1 (it could be in a tuple)
	
	def reducer_count_ratings (self, key, values): #reducer funtion is called for each unique keys, values refer to an iterable object associated with that key
		yield key, sum(values)
		
if __name__=='__main__':
	RatingsBreakdown.run()
	
class MoviesSortedByCount(MRJob)
	def steps(self): #defines a multisteps jobs (mapper, reducer)+(reducer)
		return [MRStep(mapper=self.mapper_get_movieIDs, reducer=self.reducer_count_movieIDs), MRStep(reducer=self.reducer_sort_by_count)]
		
	def mapper_get_movieIDs (self, -, line):
		(userID, movieID, rating, timestamp)=line.split('\t')
		yield movieID, 1
	
	def reducer_count_movieIDs (self, movieID, count):
		yield str(sum(count)).zfill(5), key #change the output to a string where it is filled by 0 from the left. This is intended to insure proper sorting (in hadoop streaming 10 before 2 because) in the shuffle and sort step of reducing in the next reducer function.  
	
	def reducer_sort_by_count (self, count, movies):
		for movie in movies: #some movies have similar counts, so we have to iterate over the movies that have similar counts
			yield movie, count

if __name__=='__main__':
	moviesSortedByCount.run()