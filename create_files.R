
library(purrr)
library(readr)

options(scipen=999)
test_number <- c(60,70,80)
row_interval <- test_number * 1000000
number_tests <- 1:length(test_number)

#reviews_table <- read_csv("allfiles.csv", col_names = FALSE) #, n_max=100000)

save_to_perf <- number_tests %>%
  map(~write_csv(reviews_table[1:row_interval[.x],], 
                 path=file.path("perf_files", paste("file_", test_number[.x],".csv",sep="")) ))
all_results <- NULL
file_names <- list.files(path=file.path("result_files"))
for(files in 1:length(file_names)){
  all_results <- rbind(all_results, read.csv(file.path("result_files",file_names[files])))
}
write_csv(all_results,"performance.csv")


