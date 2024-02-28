 SELECT 
  	  INST_CODE,
  	  NEWID,
  	  FISCAL_YEAR_SSI_RECOGNIZED,
  	  SSI_TOTAL_AMOUNT
  	FROM  OHIO_DW.SSI_COURSE_ALLOCATIONS
  	WHERE INST_CODE = 'OHUN'
  	AND SSI_TOTAL_AMOUNT IS NOT null