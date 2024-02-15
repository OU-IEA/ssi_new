library(DBI)
library(dplyr)
library(tidyr)
library(glue)
library(purrr)
library(readr)

source('src/helpers.R')


con <- connect('redshift')

basis_registration <- dbGetQuery(con, readr::read_file('SQL/course_compl/production_script.sql'))

ssi_totals <-
  basis_registration |> distinct(ssi_payout_y1, newid, ssi_y1_payout_total_amount) |>
  arrange(ssi_payout_y1, newid)

# Student Totals --------------------------------------------------------------


# let's calculate total number of students per matchid


# the year matrix is set up until 2035; this will create some unnecessary
# loops (see below) between now and 2035 but you won't have to update it each year;
# more years will have to be added as we get closer to 2035 (2035 - 3 = 2032);

# IMPORTANT: the status however will need to be updated manually each year.

year_matrix <- read_csv('misc/year_matrix.csv', show_col_types = FALSE)
year_matrix <- pivot_longer(year_matrix, AY_1:AY_3, values_to = "AY")

course_data <- basis_registration |>
  group_by(time_academic_yr, newid, class_course, credit_hours) |>
  summarise(n_students_per_class = n()) |>
  mutate(total_credit_hr_per_class = n_students_per_class * credit_hours)



# setting up the loop
index <- 0
datalist = list()
datalist = vector("list", length = length(unique(year_matrix$FY))) 

# actual calculations
for(year in unique(year_matrix$FY)){
  index <- index +1
  year <- as.numeric(year)
  
  # retrieving years and calculating total degrees for a given cohort
  subset_years <- year_matrix$AY[year_matrix$FY == year]
  
  temp  <- course_data |> 
     filter(time_academic_yr %in% subset_years) |> 
     group_by(newid,class_course) |> 
     summarise(cumul_total_cr_hours = sum(total_credit_hr_per_class), .groups = 'drop')
  

  temp <- temp |>
     group_by(newid) |>
     mutate(grand_total_cr_hours = sum(cumul_total_cr_hours),
            credit_hours_weight  = cumul_total_cr_hours /grand_total_cr_hours,
            fiscal_year = year) 
  

  datalist[[index]] <- temp

}

# Let's remove empty tables
  datalist <- map(datalist, function(x) {
    if (!nrow(x) == 0) {
      x
    }
  })
  
  datalist <- compact(datalist)
  results <- bind_rows(datalist)

  
final_calcs <- results |> left_join(ssi_totals, by = c('newid' = 'newid','fiscal_year' = 'ssi_payout_y1')) |>
   ungroup() |> 
   mutate(ssi_per_course_per_cr_hours = ssi_y1_payout_total_amount * credit_hours_weight/cumul_total_cr_hours) |> 
  select(newid,class_course,fiscal_year,ssi_per_course_per_cr_hours)


# Gathering & recalculating --------------------------------------------------------------

temp_1 <- basis_registration |> select(- ssi_y1_payout_total_amount) |> 
  left_join(final_calcs, by = c('newid' = 'newid', 'ssi_payout_y1' = 'fiscal_year', 'class_course' = 'class_course')) |> 
  rename(ssi_y1_payout_per_cr_hour = ssi_per_course_per_cr_hours)

temp_1 <- temp_1 |> 
  left_join(final_calcs, by = c('newid' = 'newid', 'ssi_payout_y2' = 'fiscal_year', 'class_course' = 'class_course')) |> 
  rename(ssi_y2_payout_per_cr_hour = ssi_per_course_per_cr_hours)


temp_1 <- temp_1 |> 
  left_join(final_calcs, by = c('newid' = 'newid', 'ssi_payout_y3' = 'fiscal_year', 'class_course' = 'class_course')) |> 
  rename(ssi_y3_payout_per_cr_hour = ssi_per_course_per_cr_hours)


# completeness flag
complete <- year_matrix |> distinct(FY,status)

final_ssi_courses <- temp_1 |> left_join(complete, by=c('ssi_payout_y1' = 'FY')) |>
  rename(complete_payout_y1 = status) |> 
  left_join(complete, by=c('ssi_payout_y2' = 'FY')) |> 
  rename(complete_payout_y2 = status) |> 
  left_join(complete, by=c('ssi_payout_y3' = 'FY')) |> 
  rename(complete_payout_y3 = status) |> 
  mutate(complete_flag = if_else(glue('{complete_payout_y1}-{complete_payout_y3}-{complete_payout_y1}') == 'complete-complete-complete',
                                 'complete', 'incomplete')) |> 
  select(-c(complete_payout_y1,complete_payout_y2,complete_payout_y3))

# re-coding some completes that might be in fact incompletes

final_ssi_courses$complete_flag[is.na(final_ssi_courses$ssi_y1_payout_per_cr_hour) 
                                & final_ssi_courses$complete_flag =='complete'] <- 'incomplete'

final_ssi_courses$complete_flag[is.na(final_ssi_courses$ssi_y2_payout_per_cr_hour) 
                  & final_ssi_courses$complete_flag =='complete'] <- 'incomplete'

final_ssi_courses$complete_flag[is.na(final_ssi_courses$ssi_y3_payout_per_cr_hour) 
                                & final_ssi_courses$complete_flag =='complete'] <- 'incomplete'


# Validation & Cleaning ------------------------------------------------------

# stats<- final_ssi_courses |> 
#   group_by(newid, ssi_payout_y1) |> 
#   summarise(min_y1 = min(ssi_y1_payout_per_cr_hour),
#             max_y1 = max(ssi_y1_payout_per_cr_hour),
#             min_y2 = min(ssi_y2_payout_per_cr_hour),
#             max_y2 = max(ssi_y2_payout_per_cr_hour),
#             min_y3 = min(ssi_y3_payout_per_cr_hour),
#             max_y3 = max(ssi_y3_payout_per_cr_hour))


final_ssi_courses <- final_ssi_courses |> 
  mutate(
    ssi_y1_payout_total_ssi = credit_hours * ssi_y1_payout_per_cr_hour,
    ssi_y2_payout_total_ssi = credit_hours * ssi_y2_payout_per_cr_hour,
    ssi_y3_payout_total_ssi = credit_hours * ssi_y3_payout_per_cr_hour
    
  ) 

final_ssi_courses <- final_ssi_courses |> 
  select(-inst_code) |> 
  relocate(time_academic_yr,.before = ssi_payout_y1)

final_ssi_courses <- final_ssi_courses |> 
  mutate(ssi_total_all_three_years_combined = credit_hours *(ssi_y1_payout_per_cr_hour + ssi_y2_payout_per_cr_hour + ssi_y3_payout_per_cr_hour)) |> 
  relocate(complete_flag, .after = ssi_total_all_three_years_combined) |> 
  relocate(ssi_total_all_three_years_combined,.before = complete_flag) |> 
  select(-c(new_class_number,campus_id,class_grade,
            ssi_y1_payout_per_cr_hour,ssi_y2_payout_per_cr_hour,ssi_y3_payout_per_cr_hour))


# Export ---------------------------------------------------------------------

schema <- 'ohio_dm'
table <- 'ssi_student_course_subsidy'
csv_file <- 'data_final/upd_ssi_student_course_compl_subsidy.csv'

dbCreateTable(con, Id(schema = schema, table = table), final_ssi_courses)
readr::write_csv(final_ssi_courses,csv_file,na = '')

# will truncate the original table! Modify if necessary.
upload_to_redshift(csv_file,
                   schema = schema,
                   table_name = table, truncate = TRUE)

dbDisconnect(con)
