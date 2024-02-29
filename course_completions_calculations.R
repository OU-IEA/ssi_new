library(DBI)
library(dplyr)
library(tidyr)
library(glue)
library(purrr)
library(readr)

source('src/helpers.R')


# Prep -------------------------------------------------------------------------

# setting up necessary stuff for calculations

con <- connect('redshift')
basis_registration <- dbGetQuery(con, 
                                 read_file('SQL/course_compl/course_compl.sql'))

ssi_totals <- dbGetQuery(con,
                         read_file('SQL/course_compl/course_compl_ssi_totals.sql'))

# the year matrix below is set up until 2035; this will create some unnecessary
# loops (see below) between now and 2035 but you won't have to update it each year;
# more years will have to be added as we get closer to 2035

# IMPORTANT: the status however will need to be updated manually each year.

year_matrix <- read_csv('misc/year_matrix.csv', show_col_types = FALSE)
year_matrix <- pivot_longer(year_matrix, AY_1:AY_3, values_to = "AY")


# Algorithm --------------------------------------------------------------

# the entire process can be run in one single step but given the complexity, 
# we'll break it into multiple steps for more explicit control and data checks at any given step. 


# first, let's pre-aggregate data by calculating total number of students and credits per newid in a given year

course_data <- aggregate_by_year_type(aggr_types = c('fiscal', 'calendar'),
                                      years_to_aggr = year_matrix,
                                      df = basis_registration)

# the function below does most of heavy lifting: it aggregates data 
# based on three year cohort; the year basis - fiscal or calendar is
# taken in consideration as well as actual contributing years for each fiscal year;
# the function produces different metrics that will be used to allocate SSI dollars.


results <- aggregate_cohort(years_to_aggr = year_matrix,
                            df = course_data)  

# let's bring SSI and re-allocate amounts

final_calcs <- ssi_allocate(df = results,ssi_df = ssi_totals) |> 
              bind_rows()  


temp <- final_calcs |> 
  group_by(newid, class_course,acad_year) |> 
  summarise(total_ssi = sum(ssi_per_course_per_cr_hours)) |> 
  mutate(adj_total_ssi = ifelse(acad_year %in% c(2018,2019, 2020), total_ssi * 3/4, total_ssi)) |> 
  select(-total_ssi)


final_ssi_courses |> 
  group_by(time_academic_yr) |> 
  summarise(total_amount = sum(ssi_total_all_three_years_combined, na.rm = TRUE))

ssi_totals |> 
  group_by(fiscal_year_ssi_recognized) |> 
  summarise(sum(ssi_total_amount))



## Gathering & recalculating --------------------------------------------------------------

payout_matrix <- read_csv('misc/payout_matrix.csv',show_col_types = FALSE)
basis_registration <- basis_registration |>
  add_payout_matrix(payout_df = payout_matrix)



temp_1 <- basis_registration |> 
  left_join(temp, by = c('newid' = 'newid', 'ssi_payout_y1' = 'acad_year', 'class_course' = 'class_course')) |> 
  rename(ssi_y1_payout_total_ssi = adj_total_ssi)

temp_1 <- temp_1 |> 
  left_join(temp, by = c('newid' = 'newid', 'ssi_payout_y2' = 'acad_year', 'class_course' = 'class_course')) |> 
  rename(ssi_y2_payout_total_ssi = adj_total_ssi)


temp_1 <- temp_1 |> 
  left_join(temp, by = c('newid' = 'newid', 'ssi_payout_y3' = 'acad_year', 'class_course' = 'class_course')) |> 
  rename(ssi_y3_payout_total_ssi = adj_total_ssi)


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
#  select(-inst_code) |> 
  relocate(time_academic_yr,.before = ssi_payout_y1)

final_ssi_courses <- final_ssi_courses |> 
  rowwise() |> 
  mutate(ssi_total_all_three_years_combined = sum(ssi_y1_payout_total_ssi,
                                                  ssi_y2_payout_total_ssi,
                                                  ssi_y3_payout_total_ssi,
                                                  na.rm=TRUE)) 
  

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
