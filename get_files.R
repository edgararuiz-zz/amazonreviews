
library(stringr)
library(tidyverse)

source_directory <- "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles"
all_files <- readLines("allfiles.txt")

csv_files <- all_files[!is.na(str_locate(all_files,".csv")[,1])]

csv_files <- csv_files[csv_files!="ratings_#508510.csv"] 
csv_files <- csv_files[csv_files!="ratings_.csv"] 

get_file <- function(filename)
{
  local_file <- file.path("reviewfiles", filename)
  if(!file.exists(local_file))download.file(url = file.path(source_directory, filename), destfile = local_file)
}

create_allfiles <- function(filename){
  category <- substr(filename, 9, nchar(filename))
  category <- substr(category, 1, nchar(category)-4)
  category <- str_replace_all(category, "_", " ")
  current_file <- read_csv(file.path("reviewfiles", filename), col_names=FALSE)
  current_file$category <- category
  write_csv(current_file, path="allfiles.csv", append=TRUE)
}
  


csv_files %>%
  map(~get_file(.x))

if(file.exists("allfiles.csv"))file.remove("allfiles.csv")

csv_files %>%
  map(~create_allfiles(.x))

