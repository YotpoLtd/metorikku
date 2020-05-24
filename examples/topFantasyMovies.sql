    SELECT movieId,
           title,
           avg(rating) AS averageRating
    FROM fantasyMoviesWithRatings
    GROUP BY movieId,
             title
    ORDER BY averageRating DESC
    LIMIT 100