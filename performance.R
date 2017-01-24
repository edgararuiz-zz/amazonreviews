library(dplyr)
library(readr)
library(sparklyr)
library(stringr)
library(purrr)

performance_test <-function(filename){
  test_results <- matrix(nrow=3, ncol=5)
  
  
  spark_in_memory <- system.time({
    conf <- spark_config()
    conf$`sparklyr.shell.driver-memory` <- "16G"
    sc <- spark_connect(master="local", config = conf, version="2.0.0")
    spark_read_csv(sc, "reviews" ,file.path("perf_files", filename), header = FALSE, memory=FALSE, repartition = 30) 
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
    
    test_results[2,4] <- nrow(tbl(sc,"reviews"))
    test_results[2,5] <- ncol(tbl(sc,"reviews"))
    
    spark_disconnect_all()
  })
  
  
  test_results[2,1] <- "spark_not_cached"
  test_results[2,2] <- spark_in_memory[3]
  test_results[2,3] <- spark_in_memory_table[3]

  
  
  rm(graph_table)
  rm(reviews_table)
  
  spark_in_memory <- system.time({
    conf <- spark_config()
    conf$`sparklyr.shell.driver-memory` <- "16G"
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
  
  test_results[1,1] <- "spark_in_memory"
  test_results[1,2] <- spark_in_memory[3]
  test_results[1,3] <- spark_in_memory_table[3]

  
  
  local_r  <- system.time({
    reviews_table <- read_csv(file.path("perf_files", filename), col_names = FALSE)
  })
  
  local_r_table <- system.time({
    reviews_table <- reviews_table %>%
      select(user_id = X1,
             item_id = X2,
             rating = X3,
             timestamp = X4,
             category = X5) 
    
    graph_table <- reviews_table %>%
      group_by(category) %>%
      summarise(avg_review = mean(rating),
                review_count = n()) 
    
  })
  
  test_results[3,1] <- "local_R"
  test_results[3,2] <- local_r[3]
  test_results[3,3] <- local_r_table[3]
  test_results[3,4] <- nrow(reviews_table)
  test_results[3,5] <- ncol(reviews_table)
  
  all_results <- data.frame(test_results) %>%
    select(category=X1,
           data_load=X2,
           data_wrangle=X3,
           rows=X4,
           cols=X5) %>%
    mutate(filesize=file.size(file.path("perf_files", filename))/1024/1024)
  
  write_csv(all_results,file.path("result_files", filename))

}

test_files <- c("file_60.csv" ,"file_70.csv", "file_80.csv")


test_files %>%
  map(~performance_test(.x))

