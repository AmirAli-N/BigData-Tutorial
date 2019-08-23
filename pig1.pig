ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS
	(userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')AS 
	(movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdblink:chararray);

DUMP metadata;

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;
DUMP ratingsByMovie;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

DUMP avgRatings;

DESCRIBE ratings;
DESCRIBE ratingsByMovie;
DESCRIBE avgRatings;

fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

DESCRIBE fiveStarMovies;
DESCRIBE nameLookup;

fiveStarsWithData = JOIN fiveStarMovies BY movieID, nameLookup BY movieID;

DESCRIBE fiveStarsWithData;
DUMP fiveStarsWithData;

oldestFiveStarMovies = ORDER fiveStarMovies BY nameLookup::releaseTime;

DUMP oldestFiveStarMovies;