WITH ssi_degrees AS
		(
		SELECT 
  		fiscal_year,
  		degrees as total_ssi
		FROM ohio_dw.ssi_doctoral_subsidy sds  
		WHERE inst = 'OHUN'
		),

	degree_plans AS
		(
		SELECT *
		FROM ohio_dw.d_acad_new
		WHERE eff_status = 'A'
		),


--completions + academic plans
	combo AS (
		SELECT DISTINCT 
  		stdnt_emplid,
  		acad_new.matchid,
  		completion_term,
  		time_academic_year as degree_ocurred,
  		plan_acad_plan_ld,
  		plan_acad_plan_code
		FROM perseus.completions_extract completions
		INNER JOIN degree_plans acad_new
		ON acad_new.strm = completions.completion_term 
		AND 
		acad_new.acad_plan = completions.plan_acad_plan_code 
		WHERE degree_cd = 'PHD'
		)

-- final step

	SELECT 
  	cb.stdnt_emplid,
  	cb.matchid,
  	cb.completion_term,
  	cb.plan_acad_plan_ld,
  	cb.plan_acad_plan_code,
  	cb.degree_ocurred,
	
	--years
	CAST(ssi.fiscal_year AS integer) AS ssi_payout_y1,
	CAST(ssi.fiscal_year AS integer) +1  AS ssi_payout_y2,
	CAST(ssi.fiscal_year AS integer) +2 AS ssi_payout_y3,
	
	--dollars
	round(ssi.total_ssi,1) AS ssi_y1_payout_total_amount
	
	FROM combo cb
	INNER JOIN	ssi_degrees	 ssi
	ON 
	cb.degree_ocurred = (ssi.fiscal_year-1)
  ORDER BY cb.degree_ocurred