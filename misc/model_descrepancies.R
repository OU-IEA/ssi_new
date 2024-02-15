library(DBI)
library(dplyr)
library(tidyr)
library(glue)
library(purrr)

source('src/helpers.R')
con <- connect('redshift')


ssi_alloc <- dbGetQuery(con,"
    SELECT 
  	  inst_code,
  	  newid,
  	  fiscal_year_ssi_recognized,
  	  ssi_total_amount
  	FROM  ohio_dw.ssi_course_allocations
  	WHERE inst_code = 'OHUN'
  	AND ssi_total_amount is NOT NULL
                        ")

course_inventory <- dbGetQuery( con, "

	SELECT 
		invent.*,
		invent.crse_subject ||' '||invent.crse_catalog_nbr AS class_course,
		invent.hei_subsidy_model_subj || invent.hei_course_level || invent.hei_subsidy_model_code  AS newid
	FROM ohio_dw.course_inventory invent
	
	-- this part is needed to select maximum str_version for each model
	
  INNER JOIN (
   	SELECT 
     	max(ci.strm_version) AS strm_version ,
     	ci.strm,     
     	ci.crse_subject,
     	ci.crse_catalog_nbr,
     	ci.crse_subject ||' '||ci.crse_catalog_nbr AS class_course
   	FROM ohio_dw.course_inventory ci
   	WHERE ci.hei_subsidy_eligible = 'Y'
   	GROUP BY
     	ci.strm, 
     	ci.crse_subject,
     	ci.crse_catalog_nbr
) ci
 ON invent.strm  = ci.strm AND 
    invent.strm_version = ci.strm_version AND
    invent.crse_subject  = ci.crse_subject AND 
    invent.crse_catalog_nbr = ci.crse_catalog_nbr
                                
                                ")
# this includes everything: docs and med models
ssi_newid <- ssi_alloc  |>  distinct(newid) |> pull(newid)
ci_newid <- course_inventory |> distinct(newid) |> pull(newid)

# codes that are in ssi but not course_inventory
ssi_newid[!ssi_newid %in% ci_newid] 

# codes that are in course_inventory but not in ssi
ci_newid[!ci_newid %in% ssi_newid]


