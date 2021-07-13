source("/home/thomas_wawra/projects/allianzcinema/bigRQuery.R")
source("/home/thomas_wawra/projects/allianzcinema/helper.R")
source("/home/thomas_wawra/projects/allianzcinema/etl__allianzcinemadrivein.R")

BQ_AUTH_PATH <- "/home/thomas_wawra/keys/bigdata-285708-98e44957025f.json"
BQ_AUTH_EMAIL <- "mandanten-avodi@bigdata-285708.iam.gserviceaccount.com"

bq_auth(path = BQ_AUTH_PATH, scopes = c("https://www.googleapis.com/auth/bigquery",
                                        "https://www.googleapis.com/auth/cloud-platform"))

GBQ__PROJECT_ID <<- "bigdata-285708"
GBQ__DATASET <<- "allianzcinema"
GBQ__TABLE_1 <<- "allianzcinemadrivein"
GBQ__TABLE_2 <<- "tmp_allianzcinemadrivein"
GBQ__SOURCE_URI_1 <<- sprintf("%s.%s.%s", GBQ__PROJECT_ID, GBQ__DATASET, GBQ__TABLE_1)
GBQ__SOURCE_URI_2 <<- sprintf("%s.%s.%s", GBQ__PROJECT_ID, GBQ__DATASET, GBQ__TABLE_2)

cat("get sold tickets\n")
df <- allianz_getSoldTickets()

cat("transform column names\n")
df <- df %>% helper__transform_column_names()

cat("convert datatypes\n")
df <- df %>% convert_numbers()

print(df %>% head)

cat(sprintf("check if temporary table exists: [%s]\n", GBQ__SOURCE_URI_2))
table_2_exists <- bq_table_exists(GBQ__SOURCE_URI_2)
if(table_2_exists) {
  cat(sprintf("delete temporary table: [%s]\n", GBQ__SOURCE_URI_2))
  bq_table_delete(GBQ__SOURCE_URI_2)
}

cat(sprintf("create temporary table and upload data: [%s]\n", GBQ__SOURCE_URI_2))
print(bq_table_upload(GBQ__SOURCE_URI_2, df))

cat(sprintf("check if main table exists: [%s]\n", GBQ__SOURCE_URI_1))
table_1_exists <- bq_table_exists(GBQ__SOURCE_URI_1)
if(table_1_exists) {
  cat(sprintf("delete main table: [%s]\n", GBQ__SOURCE_URI_1))
  bq_table_delete(GBQ__SOURCE_URI_1)
}

cat(sprintf("copy temporary table to main table: [%s] -> [%s]\n", GBQ__SOURCE_URI_2, GBQ__SOURCE_URI_1))
job <- bq_perform_copy(GBQ__SOURCE_URI_2, GBQ__SOURCE_URI_1, billing = GBQ__PROJECT_ID)

bq_job_wait(job)

job_status <- bq_job_status(job)

if(job_status == "DONE") {
  cat(sprintf("DONE: copy temporary table to main table: [%s] -> [%s]\n", GBQ__SOURCE_URI_2, GBQ__SOURCE_URI_1))
}

# cat("upload data to gbq\n")
# df %>% gbq_jobUpload(GBQ__PROJECT_ID, GBQ__DATASET, GBQ__TABLE)
# df %>% bigrquery::update_dataset(GBQ__PROJECT_ID, GBQ__DATASET, GBQ__TABLE)
