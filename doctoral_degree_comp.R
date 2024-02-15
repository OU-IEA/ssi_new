library(DBI)
library(dplyr)
library(tidyr)
library(glue)
library(readr)

source('src/helpers.R')

con <- connect('redshift')
basis_ssi <- dbGetQuery(con, statement = read_file('SQL/degree_compl_doctoral.sql'))



# Degree Totals --------------------------------------------------------------


# let's calculate total degrees for 
# a fy as a sum of previous three academic years

# the year matrix below is set up until 2035; this will create some unnecessary
# loops (see below) between now and 2035 but you won't have to update it each year;
# more years will have to be added as we get closer to 2035 (2035 - 3 = 2032);

# IMPORTANT: the status however will need to be updated manually each year.

year_matrix <- read_csv('misc/year_matrix.csv',show_col_types = FALSE)
year_matrix <- pivot_longer(year_matrix, AY_1:AY_3, values_to = 'AY')

degree_totals <- basis_ssi |>
  group_by(degree_ocurred, matchid) |>
  summarise(total = n())  

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
  
  results <- degree_totals |> 
    filter(degree_ocurred %in% subset_years) |> 
    group_by(matchid) |> 
    summarise(total_degrees = sum(total)) |> 
    mutate(fiscal_year = year)
  
  datalist[[index]] <- results
  
}


degrees_cohort_totals <- bind_rows(datalist)
degrees_cohort_totals <- degrees_cohort_totals |> 
  group_by(fiscal_year) |>
  summarise(total_degrees = sum(total_degrees))
  
# Joining Degrees ------------------------------------------------------------


temp <- basis_ssi |> 
  left_join(degrees_cohort_totals,by = c('ssi_payout_y1' = 'fiscal_year'))|>
  rename(ssi_payout_y1_total_degree = total_degrees)

temp <- temp |> 
  left_join(degrees_cohort_totals,by = c('ssi_payout_y2' = 'fiscal_year')) |>
  rename(ssi_payout_y2_total_degree = total_degrees)
  
temp <- temp |> 
  left_join(degrees_cohort_totals,by = c('ssi_payout_y3' = 'fiscal_year')) |> 
  rename(ssi_payout_y3_total_degree = total_degrees)


# SSI Amounts ----------------------------------------------------------------


# adding the remaining SSI amounts
  
ssi_totals <- temp |>
  distinct(ssi_payout_y1,ssi_y1_payout_total_amount) |> 
  rename(fiscal_year =  ssi_payout_y1, ssi_amount = ssi_y1_payout_total_amount) 


temp <- temp |> left_join(ssi_totals, by =c( 'ssi_payout_y2' = 'fiscal_year')) |> 
  rename('ssi_y2_payout_total_amount' = 'ssi_amount')

temp <- temp |> left_join(ssi_totals, by =c('ssi_payout_y3' = 'fiscal_year')) |> 
  rename('ssi_y3_payout_total_amount' = 'ssi_amount')

temp <- temp |> relocate(ssi_y1_payout_total_amount,.before = ssi_y2_payout_total_amount)


# Calculating Average --------------------------------------------------------


temp <- temp |> mutate('ssi_payout_avg_y1_per_degree' = round(ssi_y1_payout_total_amount/ssi_payout_y1_total_degree,1))
temp <- temp |> mutate('ssi_payout_avg_y2_per_degree' = round(ssi_y2_payout_total_amount/ssi_payout_y2_total_degree,1))
temp <- temp |> mutate('ssi_payout_avg_y3_per_degree' = round(ssi_y3_payout_total_amount/ssi_payout_y3_total_degree,1))

# adding a few additional vars
temp <- temp |> mutate('total_all_three_years_ssi_per_degree' = ssi_payout_avg_y1_per_degree + ssi_payout_avg_y2_per_degree + ssi_payout_avg_y3_per_degree)


# completeness flag
complete <- year_matrix |> distinct(FY,status)

final_ssi_degrees <- temp |> left_join(complete, by=c('ssi_payout_y1' = 'FY')) |>
  rename(complete_payout_y1 = status) |> 
  left_join(complete, by=c('ssi_payout_y2' = 'FY')) |> 
  rename(complete_payout_y2 = status) |> 
  left_join(complete, by=c('ssi_payout_y3' = 'FY')) |> 
  rename(complete_payout_y3 = status) |> 
  mutate(complete_flag = if_else(glue('{complete_payout_y1}-{complete_payout_y3}-{complete_payout_y1}') == 'complete-complete-complete',
          'complete', 'incomplete')) |> 
  select(-c(complete_payout_y1,complete_payout_y2,complete_payout_y3))

# for some matchids we may have degrees but we don't have SSI dollars

final_ssi_degrees$complete_flag[is.na(final_ssi_degrees$total_all_three_years_ssi_per_degree) & final_ssi_degrees$complete_flag =='complete'] <- 'incomplete'

# this is a check for completeness flag;
final_ssi_degrees |> distinct(degree_ocurred, ssi_payout_y1,ssi_payout_y2,ssi_payout_y3,complete_flag) |> 
  arrange(ssi_payout_y1)



# Validation & Cleaning ------------------------------------------------------


# look for anomalies; 
stats<- final_ssi_degrees |> filter(complete_flag =='complete') |> 
  group_by(degree_ocurred,matchid) |> 
  summarise(min = min(total_all_three_years_ssi_per_degree),
            max = max(total_all_three_years_ssi_per_degree),
            mean = mean(total_all_three_years_ssi_per_degree),
            median = median(total_all_three_years_ssi_per_degree))

final_ssi_degrees <- final_ssi_degrees |>
  mutate(across(ssi_y1_payout_total_amount:ssi_y3_payout_total_amount,~ round(.x,1)))



# Export ---------------------------------------------------------------------


schema <- 'ohio_dm'
table <- 'ssi_student_degree_subsidy'
csv_file <- 'data_final/ssi_doctoral_degree_subsidy.csv'

readr::write_csv(final_ssi_degrees,csv_file,na = '')

# check if you need to truncate or not; if yes, set truncate = TRUE
upload_to_redshift(csv_file,
                   schema = schema,
                   table_name = table)

dbDisconnect(con)
