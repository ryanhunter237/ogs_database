WITH summed AS (
    SELECT board_hash, SUM(freq) AS total
    FROM move_counts
    GROUP BY board_hash
)
SELECT t.threshold, COUNT(mc.*) AS remaining_rows
FROM move_counts mc
JOIN summed s ON mc.board_hash = s.board_hash
JOIN (VALUES (2), (3), (5), (10)) AS t(threshold)
    ON s.total >= t.threshold
GROUP BY t.threshold
ORDER BY t.threshold;
