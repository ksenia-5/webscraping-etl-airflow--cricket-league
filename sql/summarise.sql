SELECT
    s.BatterName,
    sixes_scored,
    fours_scored,
    total_runs,
    total_balls_faced,
    games_batted
FROM (SELECT DISTINCT(BatterName),
        SUM(Sixes) AS sixes_scored, 
        SUM(Fours) AS fours_scored,
        SUM(RunCount) AS total_runs,
        SUM(BallCount) AS total_balls_faced
    FROM scores
    WHERE LeagueYear = 2023
    GROUP BY BatterName) AS s
LEFT JOIN (SELECT 
                BatterName, 
                COUNT(BatterName) AS games_batted
            FROM scores
            WHERE LeagueYear = 2023
            GROUP BY BatterName) AS tb
ON tb.BatterName = s.BatterName
ORDER BY sixes_scored DESC
LIMIT 10;