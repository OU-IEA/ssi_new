-- SELECT DISTINCT TERMS TO MAKE SURE WE HAVE COMPARABLE UNIVERSES IN BOTH TABLES
WITH TERMS AS (
SELECT
	DISTINCT STRM
FROM
	(
	SELECT
		CCE.CLASS_NBR,
		LEN(CCE.CLASS_NBR) MAX_LENGTH,
		CCE.STUDENT_PID,
		CCE.TERM_CODE,
		REGEXP_REPLACE(CCE.TERM_CODE, 'F') AS TERM_CODE,
		TL.STRM,
		CCE.ENRL_NEWTAX_SUBJECT || CCE.ENRL_COURSE_LEVEL || CCE.ENRL_NEWTAX_MODEL AS NEWID,
		CCE.SUBSIDY_ELIGIBLE_CODE,
		CCE.COMPLETED_FLAG,
		CCE.CRSE_GRADE_OFF
	FROM
		OHIO_DW.CONV_CLASS_ENROLLMENT CCE
	INNER JOIN OHIO_DW.TERM_LOOKUP TL ON
		REGEXP_REPLACE(CCE.TERM_CODE, 'F') = TL.TERM_CODE
	WHERE
		SUBSIDY_ELIGIBLE_CODE = 1
		AND COMPLETED_FLAG = 1
		AND CCE.TERM_CODE LIKE '%F'
)),
-- NOW LET'S TAKE UNMATCHED RECORDS AND SUBSET BASED ON THE TERMS ABOVE
UNMATCHED AS (
SELECT
	NEW_CLASS_NUMBER,
	TIME_STRM ,
	TL.TERM_CODE,
	CAMPUS_ID
FROM
	ISS_PVLAD.UNMATCHED
   LEF
JOIN OHIO_DW.TERM_LOOKUP TL ON
	TIME_STRM = TL.STRM
WHERE
	TIME_STRM IN(
	SELECT
		STRM
	FROM
		TERMS)
),
-- LET'S JOIN THE RESULTS FROM ABOVE WITH ORIGNAL CONV_CLASS_ENROLLEMENT TABLE
RESULTS AS(
SELECT
	U.*,
	CCE.SUBSIDY_ELIGIBLE_CODE,
	CCE.COMPLETED_FLAG,
	CCE.TERM_CODE AS ORIGINAL_TERM_CODE
FROM
	UNMATCHED U
LEFT JOIN OHIO_DW.CONV_CLASS_ENROLLMENT CCE 
ON
	U.NEW_CLASS_NUMBER = CCE.CLASS_NBR
	AND U.TERM_CODE = CCE.TERM_CODE
	AND U.CAMPUS_ID = CCE.STUDENT_PID
)
-- THIS ARE JUST PIVOTED RESULTS FROM ABOVE

SELECT
	*
FROM 
	(
	SELECT 
			SUBSIDY_ELIGIBLE_CODE, 
			COMPLETED_FLAG, 
			CAMPUS_ID
	FROM
		RESULTS
	) PIVOT (
	COUNT(CAMPUS_ID) FOR COMPLETED_FLAG IN (0, 1, NULL)
	)

 
/*	NULLS ARE 'DROPOUT'
 	COMPLETED_FLAG (COLUMNS) 0 NOT COMPLETED; 1 COMPLETED.
	SUBSIDY ELIGIBLE (ROWS):
		2	NOT ELIGIBLE
		1	ELIGIBLE
 */