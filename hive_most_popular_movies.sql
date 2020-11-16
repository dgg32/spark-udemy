SELECT movie_name, count(rating), avg(rating)
from ratings, movie_names
where ratings.movie_id = movie_names.movie_id

GROUP BY movie_name
HAVING count(rating) > 10
ORDER BY avg(rating) DESC;


CREATE VIEW IF NOT EXISTS avgRatings1 AS
SELECT ratings.movie_id, avg(rating) as avgRating, count(movie_id) as ratingCount
FROM ratings
GROUP BY movie_id
ORDER BY avgRating DESC;

SELECT n.movie_name, avgRating
FROM avgRatings t JOIN movie_names n ON t.movie_id = n.movie_id
WHERE ratingCount > 10;