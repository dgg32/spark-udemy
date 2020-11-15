ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
 AS (movieID:int, movieTitle:chararray, releaseDate:chararray);
 
nameLookup = FOREACH metadata GENERATE movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;

--https://stackoverflow.com/questions/29808460/how-does-group-as-work-in-pig
avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS ratingCount;

badMovies = FILTER avgRatings BY avgRating < 2;

badMoviesWithData = JOIN badMovies By movieID, nameLookup BY movieID;

badMoviesWithDataSelect = FOREACH badMoviesWithData GENERATE nameLookup::movieTitle 
AS movieName, badMovies::avgRating AS avgRating, badMovies::ratingCount AS ratingCount;

badMoviesPopular = ORDER badMoviesWithDataSelect BY ratingCount DESC;

DUMP badMoviesPopular;
