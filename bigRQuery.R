library(googleAuthR)
library(bigrquery)
library(openxlsx)
library(dplyr)
library(lubridate)
library(readr)
library(stringr)
library(jsonlite)
# library(notifier)


#BQ_AUTH_PATH <- "/home/thomas_wawra/keys/mandanten-305694dc610f.json"
#BQ_AUTH_EMAIL <- "twawra@gmail.com"

#bq_auth(path = BQ_AUTH_PATH, scopes = c("https://www.googleapis.com/auth/bigquery"))

visits <- function(start.date = Sys.Date() - days(1),
                                end.date = Sys.Date() - days(1)) {
  # adticket
  #cat(sprintf("extract visits: %s[start.date] %s[end.date] [adticket]\n", start.date, end.date))
  #gbq_visits(
    #view.id = view_id.adticket,
    #owner = "adticket",
    #start.date = start.date,
    #end.date = end.date
  #)
  # reservix
  cat(sprintf("extract visits: %s[start.date] %s[end.date] [reservix]\n", start.date, end.date))
  gbq_visits(
    view.id = view_id.universal,
    owner = "starticket",
    start.date = start.date,
    end.date = end.date
  )
}

gbq_visits <- function(view.id = view_id.universal,
                       start.date = Sys.Date() - days(1),
                       end.date = Sys.Date() - days(1),
                       dimensions = c("ga:date",
                                      "ga:landingPagePath",
                                      "ga:medium",
                                      "ga:sourceMedium",
                                      "ga:region",
                                      "ga:city",
                                      "ga:country"),
                       metrics = c("ga:users",
                                   "ga:sessions",
                                   "ga:pageviews",
                                   "ga:bounceRate"),
                       owner = "starticket",
                       save = TRUE,
                       path = "~/tmp/data/visits/") {

  df <- get_ga(
    profileId = view.id,
    start.date = start.date,
    end.date = end.date,
    dimensions = dimensions,
    metrics = metrics,
    samplingLevel = "HIGHER_PRECISION",
    fetch.by = "day")

  df$date <- as.Date(df$date) + days(1)
  df$owner <- owner

  if(save) {
    uri <- sprintf("%svisits_%s_%s_%s.csv",
                   path,
                   owner,
                   format(as.Date(start.date), format("%Y%m%d")),
                   format(as.Date(end.date), format("%Y%m%d")))
    cat(sprintf("writing results: %s\n", uri))
    write_delim(df, uri, delim = "\t")
  }
}

gbq_stringsToLower <- function(df, isSales = FALSE) {
  df$eventName <- tolower(df$eventName)
  df$eventGroupName <- tolower(df$eventGroupName)
  df$eventStreet <- tolower(df$eventStreet)
  df$eventCity <- tolower(df$eventCity)
  df$eventTypeName <- tolower(df$eventTypeName)
  df$customerStreet <- tolower(df$customerStreet)
  df$customerCity <- tolower(df$customerCity)
  df$organizerName <- tolower(df$organizerName)
  df$organizerCity <- tolower(df$organizerCity)
  if(isSales == TRUE) {
    df$adviser <- tolower(df$adviser)
  }
  return(df)
}

gbq_cleanUpSchema_rawRevenue_Organizer <- function(df) {
  cat("schema: prepare\n")
  df$vId <- as.integer(str_trim(df$vId))
  df$fId <- as.integer(str_trim(df$fId))
  df$pVid <- as.integer(str_trim(df$pVid))
  df$adviser <- as.character(str_trim(df$adviser)) %>% tolower
  df$canvasser <- as.character(str_trim(df$canvasser)) %>% tolower
  df$organizerName <- as.character(str_trim(df$organizerName)) %>% tolower
  df$organizerZip <- as.character(str_trim(df$organizerZip))
  df$eventId <- as.integer(str_trim(df$eventId))
  df$eventName <- as.character(str_trim(df$eventName)) %>% tolower
  df$eventGroupId <- as.integer(str_trim(df$eventGroupId))
  df$eventGroupName <- as.character(str_trim(df$eventGroupName)) %>% tolower
  df$eventZip <- as.character(str_trim(df$eventZip))
  df$eventZip <- as.character(str_trim(df$eventZip))
  df$eventCity <- as.character(str_trim(df$eventCity)) %>% tolower
  df$eventLongitude <- as.double(str_trim(df$eventLongitude))
  df$eventLatitude <- as.double(str_trim(df$eventLatitude))
  df$tickets <- as.integer(str_trim(df$tickets))
  df$soldTickets <- as.integer(str_trim(df$soldTickets))
  df$totalRevenue <- as.double(str_trim(df$totalRevenue))
  df$advanceBookingFee <- as.double(str_trim(df$advanceBookingFee))
  df$refundingFee <- as.double(str_trim(df$refundingFee))
  df$fee1 <- as.double(str_trim(df$fee1))
  df$fee2 <- as.double(str_trim(df$fee2))
  df$fee3 <- as.double(str_trim(df$fee3))
  df$fee4 <- as.double(str_trim(df$fee4))
  df$fee5 <- as.double(str_trim(df$fee5))
  df$fee6 <- as.double(str_trim(df$fee6))
  df$systemFee <- as.double(str_trim(df$systemFee))
  df$systemFeeAdticket <- as.double(str_trim(df$systemFeeAdticket))
  df$article <- as.double(str_trim(df$article))
  df$adding <- as.double(str_trim(df$adding))
  df$discount <- as.double(str_trim(df$discount))
  df$creditVoucher <- as.double(str_trim(df$creditVoucher))
  df$deliveryFee <- as.double(str_trim(df$deliveryFee))
  df$cancellationFee <- as.double(str_trim(df$cancellationFee))
  df$cashOnDelivery <- as.double(str_trim(df$cashOnDelivery))

  return(df)
}

gbq_cleanUpSchema_rawRevenue <- function(df, owner = "starticket", isSales = FALSE, convertDate = FALSE) {
  # df$date <- df$date %>% convertToDate
  if(convertDate) {
    df$date <- as.POSIXct(df$date, tz = "Europe/Berlin")
  }
  df$orderId <- as.integer(df$orderId)
  df$countTickets <- as.integer(df$countTickets)
  df$systemFee <- as.double(df$systemFee)
  df$advanceBookingFee <- as.double(df$advanceBookingFee)
  df$refundingFee <- as.double(df$refundingFee)
  df$articleFee <- as.double(df$articleFee)
  df$deliveryFee <- as.double(df$deliveryFee)
  df$netPrice <- as.double(df$netPrice)
  df$grossPrice <- as.double(df$grossPrice)
  df$total <- as.double(df$total)
  df$organizerId <- as.integer(df$organizerId)
  df$organizerLongitude <- as.double(df$organizerLongitude)
  df$organizerLatitude <- as.double(df$organizerLatitude)
  df$eventId <- as.integer(df$eventId)
  df$eventLon <- as.double(df$eventLon)
  df$eventLat <- as.double(df$eventLat)
  df$eventGroupId <- as.integer(df$eventGroupId)
  df$eventType <- as.integer(df$eventType)
  df$eventTypeGroup <- as.integer(df$eventTypeGroup)
  df$vId <- as.character(df$vId)
  df$fId <- as.character(df$fId)
  df$owner <- owner

  if(isSales == TRUE) {
    df$adviser <- as.character(df$adviser)
  } else {
    df <- df[,names(df) != "adviser"]
  }

  return(df)
}

gbq_jobUpload_rawRevenue <- function(path,
                                     projectId = "st-production-839842938918",
                                     dataset = "revenuestore",
                                     table = "raw",
                                     owner = "starticket",
                                     createDisposition = "CREATE_IF_NEEDED",
                                     writeDisposition = "WRITE_APPEND",
                                     isSales = FALSE,
                                     convertDate = FALSE) {
  cat("importing xlxs\n")
  df <- read.xlsx(path)
    
  cat("clean up schema\n")
  df <- gbq_cleanUpSchema_rawRevenue(df, owner, isSales = isSales, convertDate = convertDate)

  cat("to lower\n")
  df <- gbq_stringsToLower(df, isSales = isSales)

  # import to GoogleBigQuery Project
  df %>% gbq_jobUpload(projectId = projectId,
                       dataset = dataset,
                       table = table,
                       createDisposition = createDisposition,
                       writeDisposition = writeDisposition,
                       owner = owner)
}


gbq_jobUpload <- function(df,
                          projectId = "st-production",
                          dataset = "gsc",
                          table = "starticket_www_ch",
                          createDisposition = "CREATE_IF_NEEDED",
                          writeDisposition = "WRITE_APPEND") {

  #bigQueryR::bqr_auth(new_user = FALSE, no_auto = TRUE)

  cat(paste(sprintf("%s rows to append", nrow(df)), "\n", sep = ""))
  cat(
    sprintf(
      "job upload: %s|%s:%s [%s|%s]\n",
      projectId,
      dataset,
      table,
      createDisposition,
      writeDisposition
    )
  )
    
  job <-
    insert_upload_job(
      projectId,
      dataset,
      table,
      df,
      create_disposition = createDisposition,
      write_disposition = writeDisposition
    )

  wait_for(job)
  
  # notify(sprintf("finished upload: %s | %s | %s", projectId, dataset, table))
}


gbq_jobUpload__byList <- function(data, projectId = "mandanten", dataset = "haka", table) {
  
  chunks <- length(data)
  
  counter <- 1
  
  data %>% lapply(function(chunk) {
    
    if(nrow(chunk) > 0) {
      
      cat(sprintf("[#%s] upload chunk #%s\n", chunks, counter))
      
      chunk %>% gbq_jobUpload(projectId, dataset, table)
      
      counter <<- counter + 1
    }
  })
}
