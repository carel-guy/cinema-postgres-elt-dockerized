WITH films_with_ratings AS (
    SELECT
        f.film_id,
        f.title,
        f.release_date,
        f.price,
        f.rating,
        f.user_rating,
        CASE
            WHEN f.user_rating >= 4.5 THEN 'Excellent'
            WHEN f.user_rating >= 3.5 THEN 'Bon'
            WHEN f.user_rating >= 2.5 THEN 'Moyen'
            ELSE 'Faible'
        END AS rating_category
    FROM {{ ref('films') }} AS f
),

actor_rollup AS (
    SELECT
        fa.film_id,
        COUNT(DISTINCT fa.actor_id) AS actor_count,
        STRING_AGG(a.actor_name, ', ' ORDER BY a.actor_name) AS actors
    FROM {{ ref('film_actors') }} fa
    LEFT JOIN {{ ref('actors') }} a ON fa.actor_id = a.actor_id
    GROUP BY fa.film_id
),

actor_scores AS (
    SELECT
        fa.actor_id,
        AVG(f.user_rating) AS avg_rating
    FROM {{ ref('film_actors') }} fa
    JOIN {{ ref('films') }} f ON fa.film_id = f.film_id
    GROUP BY fa.actor_id
),

actor_performance AS (
    SELECT
        fa.film_id,
        AVG(ascores.avg_rating) AS avg_actor_rating
    FROM {{ ref('film_actors') }} fa
    JOIN actor_scores ascores ON fa.actor_id = ascores.actor_id
    GROUP BY fa.film_id
)

SELECT
    fwr.film_id,
    fwr.title,
    fwr.release_date,
    fwr.price,
    fwr.rating,
    fwr.user_rating,
    fwr.rating_category,
    COALESCE(ar.actors, 'Acteurs non référencés') AS actors,
    COALESCE(ar.actor_count, 0) AS actor_count,
    ROUND(
        COALESCE(actor_perf.avg_actor_rating, fwr.user_rating)::numeric,
        2
    ) AS avg_actor_rating
FROM films_with_ratings fwr
LEFT JOIN actor_rollup ar ON fwr.film_id = ar.film_id
LEFT JOIN actor_performance actor_perf ON fwr.film_id = actor_perf.film_id
