SELECT groupArray(points) AS pointsArr
FROM (
	SELECT *
	FROM db_bruteforce.tbl_results
	WHERE oracle_name='{oracle_name}' AND experiment_name = '{experiment_name}'
	ORDER BY ts_start_ms
)