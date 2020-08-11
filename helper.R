# library(tidyverse)  
library(futile.logger)
library(stringr)
library(dplyr)
library(jsonlite)

# httr::set_config(config(ssl_verifypeer = 0L))


#####################################################################
# helper
#####################################################################

extract_numbers <- function(numbers)
{
  x <- gregexpr("[0-9]+", numbers)
  x <- as.numeric(unlist(regmatches(numbers, x)))
  return(x)
}

dedup_byFeatures <- function(df, features)
{
  df <- df[!duplicated(df[, features]), ]
  
  return(df)
}

collapse_featureList <- function(df, features)
{
  lapply(features, function(feature)
  {
    for (i in 1:nrow(df))
    {
      df[i, feature] <- df[i, feature] %>%
        unlist %>% FUN =
          paste(collapse = "|")
    }
  })
  
  return(df)
}

generate_dataframe_chunks <- function(df, chunk = 5000)
{
  n <- nrow(df)
  r <- rep(1:ceiling(n / chunk), each = chunk)[1:n]
  d <- split(df, r)
  return(d)
}

iconv_remove_nonValid_utf8 <- function(string)
{
  if (!is.null(string))
  {
    string <- stringi::stri_replace_all_fixed(
      string,
      c("ä", "ö", "ü", "", "", "", ""),
      c("ae", "oe", "ue", "ss", "Ae", "Oe", "Ue"),
      vectorize_all = FALSE
    )
    
    string <-
      iconv(string,
            from = "UTF-8",
            to = "ASCII",
            toRaw = FALSE)
  } else
  {
    string = "NA"
  }
  
  return(string)
}

cleanFiles <- function(file, newfile)
{
  writeLines(iconv(readLines(file, skipNul = TRUE)), newfile)
}

cleanRows <- function(df)
{
  for (i in 1:nrow(df))
  {
    df[i, ] <- iconv(df[i, ])
    
    
  }
}

details <- function(x)
{
  details <-
    list(
      x = x,
      encoding = Encoding(x),
      bytes = nchar(x, "b"),
      chars = nchar(x, "c"),
      width = nchar(x, "w"),
      raw = paste(charToRaw(x), collapse = ":")
    )
  print(t(as.matrix(details)))
}

convert_ascii_blanks <- function(string,
                                 ascii_blankToReplace = 32,
                                 ascii_blankReplacement = 160)
{
  print(substr(string, 1, gregexpr(pattern = ' ', string)))
  
  string_1 <-
    substr(string, 1, gregexpr(pattern = ' ', string)[[1]][1]) %>%
    charToRaw %>%
    as.integer
  
  string_2 <-
    substr(string, gregexpr(pattern = ' ', string)[[1]][1] + 1, nchar(string))
  
  string_1[string_1 == ascii_blankToReplace] <-
    ascii_blankReplacement
  
  string_1 <- string_1 %>%
    as.raw %>%
    rawToChar
  
  string <- paste(string_1, string_2, sep = "")
  
  return(string)
}

check_iconv_to <- function(string)
{
  results <- lapply(iconvlist(), function(to)
  {
    res <- data_frame(
      origin = string,
      from = Encoding(string),
      to = to,
      res = try(iconv(string, from = Encoding(string), to = to), silent = TRUE)
    )
    
    
    return(res)
  }) %>% bind_rows
  
  return(results)
}

multiplyRows_byN <- function(df,
                             feature)
{
  df_multipliedRows <- data_frame()
  
  for (i in 1:nrow(df))
  {
    cat(sprintf("%s\n", i))
    
    df_multipliedRow <-
      as.data.frame(lapply(df[i, ], rep, df[i, feature]))
    
    df_multipliedRows <-
      bind_rows(df_multipliedRows, df_multipliedRow)
  }
  
  return(df_multipliedRows)
}

#################################################################
# helper get contents of response
#################################################################
getContents <- function(response) {
  contents <- httr::content(response, as = "text", encoding = "UTF-8") %>%
    fromJSON
  
  return(contents)
}

getContents_HTML <- function(response) {
  contents <- httr::content(response, as = "text", encoding = "UTF-8")
  
  return(contents)
}

nwords <- function(string, pseudo=F){
  ifelse( pseudo, 
          pattern <- "\\S+", 
          pattern <- "[[:alpha:]]+" 
  )
  str_count(string, pattern)
}

'%ni%' <- Negate('%in%')

splitUrlfixed <- function(df,
                          pattern = "/",
                          n=10,
                          feature = "landingPagePath") {

  df_dirs <- lapply(df[,feature], function(p) {
    
    res <- str_split_fixed(p, pattern, n) %>% 
      as.data.frame(stringsAsFactors=FALSE)
    
    return(res)
  }) %>% bind_rows
  
  df <- bind_cols(df, df_dirs)
  
  return(df)
}