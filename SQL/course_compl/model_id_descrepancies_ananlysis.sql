/*This script takes data from course_registration extract and joins it with data from conv_class_enrollement;
in addition, we are taking data from course_inventory to compare ssi model ids.
This is a good way to identify those discrepancies.
*/

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
      	TIME_ACADEMIC_YR
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
    CCE.ENRL_NEWTAX_SUBJECT || CCE.ENRL_COURSE_LEVEL ||CCE.ENRL_NEWTAX_MODEL AS NEWID,
    CCE.SUBSIDY_ELIGIBLE_CODE,
    CCE.COMPLETED_FLAG,
    CCE.CRSE_GRADE_OFF 
  FROM 
    OHIO_DW.CONV_CLASS_ENROLLMENT  CCE
  INNER JOIN OHIO_DW.TERM_LOOKUP TL ON REGEXP_REPLACE(CCE.TERM_CODE,'F')  = TL.TERM_CODE 
   -- AND COMPLETED_FLAG = 1  SUBMITS ANOTHER FILE TO ODHE BECAUSE SOME GRADES GET UPDATED;
   -- THIS DOESN'T REALLY MATTER BUT STILL, LET'S REMOVE THIS FLAG.
  WHERE SUBSIDY_ELIGIBLE_CODE = 1  AND CCE.TERM_CODE LIKE '%F'
 ),
 -- 3) 1+2
MERGED AS  (
 SELECT 
	    CR.*,
	    CLE.TERM_CODE,
	    REGEXP_REPLACE(REGEXP_REPLACE(CLE.NEWID,'DOC','Doc'), 'MED', 'Med') AS NEWID, -- SOME CLEANING
	    CLE.SUBSIDY_ELIGIBLE_CODE,
	    CLE.COMPLETED_FLAG,
	    CLE.CRSE_GRADE_OFF
    FROM COURSE_REGISTRATION CR
    LEFT JOIN CONV_CLASS_ENROLL CLE 
    ON CR.TIME_STRM = CLE.STRM 
    AND CR.NEW_CLASS_NUMBER = CLE.CLASS_NBR
    AND CR.CAMPUS_ID = CLE.STUDENT_PID
),
 -- 4) LET'S GRAB COURSE INVENTORY
	CLASS_INVENTORY AS
		(
	SELECT 
		INVENT.*,
		INVENT.CRSE_SUBJECT ||' '||INVENT.CRSE_CATALOG_NBR AS CLASS_COURSE,
		INVENT.HEI_SUBSIDY_MODEL_SUBJ || INVENT.HEI_COURSE_LEVEL || INVENT.HEI_SUBSIDY_MODEL_CODE  AS NEWID
	FROM OHIO_DW.COURSE_INVENTORY INVENT
	
	-- THIS PART IS NEEDED TO SELECT MAXIMUM STR_VERSION FOR EACH MODEL
	
  INNER JOIN (
   	SELECT 
     	MAX(CI.STRM_VERSION) AS STRM_VERSION ,
     	CI.STRM,     
     	CI.CRSE_SUBJECT,
     	CI.CRSE_CATALOG_NBR,
     	CI.CRSE_SUBJECT ||' '||CI.CRSE_CATALOG_NBR AS CLASS_COURSE
   	FROM OHIO_DW.COURSE_INVENTORY CI
   	WHERE CI.HEI_SUBSIDY_ELIGIBLE = 'Y'
   	GROUP BY
     	CI.STRM, 
     	CI.CRSE_SUBJECT,
     	CI.CRSE_CATALOG_NBR
) CI
 ON INVENT.STRM  = CI.STRM AND 
    INVENT.STRM_VERSION = CI.STRM_VERSION AND
    INVENT.CRSE_SUBJECT  = CI.CRSE_SUBJECT AND 
    INVENT.CRSE_CATALOG_NBR = CI.CRSE_CATALOG_NBR
		)
-- 5) LET'S JOIN 4 WITH STEP 3 TO COMPARE MODEL NEWIDS

SELECT 
COUNT(*) AS TOTALN,
COUNT(NEWID) NON_EMPTY
FROM (
SELECT DISTINCT 
        M.*,
        INVENT.NEWID AS CI_NEWID,
        CASE WHEN M.NEWID != INVENT.NEWID THEN 0 ELSE 1 END AS NEWID_EQUAL 
    FROM MERGED M  
    LEFT JOIN CLASS_INVENTORY INVENT
    ON M.CLASS_COURSE = INVENT.CLASS_COURSE 
    AND M.TIME_STRM = INVENT.STRM
)
 