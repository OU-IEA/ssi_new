library(aws.s3)
library(RPostgres)
library(glue)
library(keyring)
library(ROracle)

# connecting to the server
connect <- function(db = c("edwd", "csprd", "odwp","redshift"), config_path,...) {
  
  Sys.setenv(OCI_LIB64 = "C:/Oracle")
  
  if (missing(config_path)) {
    config_path <- "src/config.yml"
  } else{
    config_path <- config_path
  }
  
  config <- config::get(file = config_path)
  type <- match.arg(db)
  
  username <- paste0(type, "_username")
  password <- paste0(type, "_password")
  
  if (type == 'redshift') {
    dbConnect(drv  = RPostgres::Postgres(),
              dbname = config[['amazon_db']],
              host = config[['amazon_server']],
              port = config[['amazon_db_port']],
              user = config[[username]],
              password = config[[password]])
    
  } else{
    
    dbConnect(
      dbDriver("Oracle"),
      config[[username]],
      config[[password]],
      type
    )}
}


to_excel <- function(.x){
  file <-paste(tempfile(),'.csv')
  data.table::fwrite(.x,file)
  browseURL(file)
  
}



upload_to_redshift <- function(file_path, schema, table_name, truncate = FALSE){
  
  # setting up the credentials
  
  AWS_ACCESS_KEY_ID <- key_get('s3_bucket','access_key_id')
  AWS_SECRET_ACCESS_KEY <- key_get('s3_bucket','secret_access_key')
  AWS_SERVER <- key_get('redshift','host')
  AWS_USERNAME <- key_get('redshift','username')
  AWS_PASSWORD <- key_get('redshift','password')
  AWS_DB <- key_get('redshift','db')
  S3_BUCKET <- key_get('s3_bucket','bucket_name')
  
  
  # Connect to Redshift
  
  con <- dbConnect(
    Postgres(),
    dbname = AWS_DB,
    host = AWS_SERVER,
    port = 5439,
    user = AWS_USERNAME,
    password = AWS_PASSWORD,
    sslmode = 'require'
  )
  
  
  # S3 Bucket 
  
  secret_key <- AWS_SECRET_ACCESS_KEY
  access_key <- AWS_ACCESS_KEY_ID
  s3_bucket <-  S3_BUCKET
  region <- 'us-east-1'
  
  object_name <- tail(strsplit(file_path,split = '/')[[1]],n = 1)
  table_name <- table_name
  schema <- schema
  
  
  # Upload to S3 
  tryCatch(
    expr = {
      put_object(file = file_path, 
                 bucket = s3_bucket, 
                 object = object_name,
                 key = access_key,
                 secret = secret_key,
                 region =  region,
                 multipart = TRUE)
      
      print('Uploaded to S3')
    }, error = function(err){
      print(err)
    }
  )
  
  # Upload to Redshift 
  
  trunc_query <-  glue("truncate table {schema}.{table_name};")
  copy_query <-  glue("copy {schema}.{table_name} from 's3://{s3_bucket}/{object_name}' credentials 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}' csv  IGNOREHEADER as 1 TRIMBLANKS BLANKSASNULL IGNOREBLANKLINES DATEFORMAT 'auto' TIMEFORMAT 'auto' NULL as 'NULL' ACCEPTINVCHARS TRUNCATECOLUMNS;")
  
  if(truncate){
    
    dbExecute(con, trunc_query)
	  print('Truncating table first...')
  }
  
  dbExecute(con, copy_query)
  
  dbDisconnect(con)
  
  
}


# example of how to use it

# insert data without truncating a table
 # upload_to_redshift("C:\\Users\\pascalv\\Desktop\\course_inventory.csv",
 #                     schema = 'iss_pvlad',
 #                     table_name = 'course_inventory')

# insert data with truncating a table first
# upload_to_redshift("C:/Users/pascalv/Documents/Projects/Redshift/files/covid19_cvs_tests.csv",
#                    schema = 'iss_pvlad',
#                    table_name = 'covid19_cvs_tests_csv',
#                    truncate = TRUE)
