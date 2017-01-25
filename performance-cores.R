library(dplyr)
library(readr)
library(sparklyr)
library(stringr)
library(purrr)

performance_test <-function(filename, cores){
  
  test_results <- matrix(nrow=1, ncol=5)
  
  spark_in_memory <- system.time({
    conf <- spark_config()
    conf$`sparklyr.shell.driver-memory` <- "16G"
    conf$`sparklyr.cores.local` <- cores
    sc <- spark_connect(master="local", config = conf, version="2.0.0")
    spark_read_csv(sc, "reviews" ,file.path("perf_files", filename), header = FALSE, memory=TRUE, repartition = 30) 
  })
  
  spark_in_memory_table <- system.time({
    reviews_table <- tbl(sc,"reviews") %>%
      select(user_id = V1,
             item_id = V2,
             rating = V3,
             timestamp = V4,
             category = V5) 
    
    graph_table <- reviews_table %>%
      group_by(category) %>%
      summarise(avg_review = mean(rating),
                review_count = n()) %>%
      collect()
    
    test_results[1,4] <- nrow(tbl(sc,"reviews"))
    test_results[1,5] <- ncol(tbl(sc,"reviews"))
    
    spark_disconnect_all()
  })
  
  rm(graph_table)
  rm(reviews_table)
  
  test_results[1,1] <- cores
  test_results[1,2] <- spark_in_memory[3]
  test_results[1,3] <- spark_in_memory_table[3]
  
  all_results <- data.frame(test_results) %>%
    select(core_number=X1,
           data_load=X2,
           data_wrangle=X3,
           rows=X4,
           cols=X5) %>%
    mutate(filesize=file.size(file.path("perf_files", filename))/1024/1024)
  
  write_csv(all_results,file.path("core_performance",paste(cores,filename, sep="-")))
  
}

test_files <- 1:4
test_files %>%
  map(~performance_test("file_70.csv",.x))


all_results <- NULL
file_names <- list.files(path=file.path("core_performance"))
for(files in 1:length(file_names)){
  all_results <- rbind(all_results, read.csv(file.path("core_performance",file_names[files])))
}


write_csv(all_results,"core_performance.csv")




