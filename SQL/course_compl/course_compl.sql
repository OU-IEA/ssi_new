WITH COURSE_REGISTRATION AS

-- 1) COURSE REGISTRATION
		(
SELECT * FROM(
	SELECT DISTINCT
		CLASS_CRSE_ID,
		CLASS_NUMBER,
		LPAD(CLASS_NUMBER, 5, 0) NEW_CLASS_NUMBER,
      	TIME_STRM,
      	CLASS_COURSE,
      	CLASS_TITLE_LD,
      	STDNT_EMPLID,
      	CAMPUS_ID,
      	CLASS_NUMBER || CLASS_COURSE || TIME_STRM || STDNT_EMPLID AS UNIQ_COMBO_ID,
      	CREDIT_HOURS,
      	CLASS_GRADE,
      	CASE
	      	WHEN CLASS_GRADE IN ('A','A-','B+', 'B','B-','C+','C','C-','D+','D','D-','CR','P','S') THEN 'Y' 
	      	WHEN CLASS_GRADE IN ('PR') THEN 
	      			CASE WHEN STDNT_ACAD_CAREER_CD = 'UGRD' THEN 'N' ELSE 'Y'
	      			END
	      	ELSE 'N'		
      	END AS PASSING_GRADE,
      	CAST(TIME_ACADEMIC_YR AS INTEGER) TIME_ACADEMIC_YR
    FROM PERSEUS.COURSE_REGISTRATIONS_EXTRACT
    WHERE
    CREDIT_HOURS > 0
    ) 
    WHERE PASSING_GRADE = 'Y'
),

CONV_CLASS_ENROLL AS 

-- 2) CONV_CLASS_ENROLL + OHIO_DW.TERM_LOOKUP TO GET STERM
  (
  SELECT
    CCE.CLASS_NBR,
    CCE.STUDENT_PID,
    CCE.TERM_CODE,
    REGEXP_REPLACE(CCE.TERM_CODE,'F') AS UPD_TERM_CODE, 
    TL.STRM, 
    CAST(TL.TERM_YEAR AS INTEGER) AS CALENDAR_YEAR,
    CCE.ENRL_NEWTAX_SUBJECT || CCE.ENRL_COURSE_LEVEL ||CCE.ENRL_NEWTAX_MODEL AS NEWID,
    CCE.SUBSIDY_ELIGIBLE_CODE,
    CCE.COMPLETED_FLAG,
    CCE.CRSE_GRADE_OFF 
  FROM 
    OHIO_DW.CONV_CLASS_ENROLLMENT  CCE
  INNER JOIN OHIO_DW.TERM_LOOKUP TL ON REGEXP_REPLACE(CCE.TERM_CODE,'F')  = TL.TERM_CODE 
   -- AND COMPLETED_FLAG = 1  
   -- LINE ABOVE WAS COMMENTED OUT BECAUSE KAREN SUBMITS ANOTHER FILE TO ODHE... BECAUSE SOME GRADES GET UPDATED;
   -- THIS DOESN'T REALLY MATTER BUT STILL, LET'S REMOVE THIS FLAG.
  WHERE SUBSIDY_ELIGIBLE_CODE = 1  AND CCE.TERM_CODE LIKE '%F'
 )
 
 -- 3) adding 1 + 2
 
 SELECT 
	    CR.*,
	    CLE.CALENDAR_YEAR,
	    REGEXP_REPLACE(REGEXP_REPLACE(CLE.NEWID,'DOC','Doc'), 'MED', 'Med') AS NEWID -- SOME CLEANING
    FROM COURSE_REGISTRATION CR
    INNER JOIN CONV_CLASS_ENROLL CLE 
    ON CR.TIME_STRM = CLE.STRM 
    AND CR.NEW_CLASS_NUMBER = CLE.CLASS_NBR
    AND CR.CAMPUS_ID = CLE.STUDENT_PID

