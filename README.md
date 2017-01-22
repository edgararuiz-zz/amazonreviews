    library(dplyr)
    library(readr)
    library(sparklyr)
    library(stringr)

    test_results <- matrix(nrow=3, ncol=3)

### Spark - Not Cached

    spark_in_memory <- system.time({
      conf <- spark_config()
      conf$`sparklyr.shell.driver-memory` <- "16G"
      sc <- spark_connect(master="local", config = conf)
      spark_read_csv(sc, "reviews" ,"allfiles.csv", header = FALSE, memory=FALSE, repartition = 30) 
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
      

      spark_disconnect_all()
    })

    rm(graph_table)
    rm(reviews_table)

    test_results[2,1] <- "spark_not_cached"
    test_results[2,2] <- spark_in_memory[3]
    test_results[2,3] <- spark_in_memory_table[3]

### Spark Cached

    spark_in_memory <- system.time({
      conf <- spark_config()
      conf$`sparklyr.shell.driver-memory` <- "16G"
      sc <- spark_connect(master="local", config = conf)
      spark_read_csv(sc, "reviews" ,"allfiles.csv", header = FALSE, memory=TRUE, repartition = 30) 
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
      

      spark_disconnect_all()
    })

    rm(graph_table)
    rm(reviews_table)

    test_results[1,1] <- "spark_in_memory"
    test_results[1,2] <- spark_in_memory[3]
    test_results[1,3] <- spark_in_memory_table[3]

### Loal R

    local_r  <- system.time({
      reviews_table <- read_csv("allfiles.csv", col_names = FALSE)
    })

    ## Parsed with column specification:
    ## cols(
    ##   X1 = col_character(),
    ##   X2 = col_character(),
    ##   X3 = col_double(),
    ##   X4 = col_integer(),
    ##   X5 = col_character()
    ## )

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

    all_results <- data.frame(test_results) %>%
      select(category=X1,
             data_load=X2,
             data_wrangle=X3)

    print(all_results)

    ##           category data_load     data_wrangle
    ## 1  spark_in_memory   112.666 5.02700000000002
    ## 2 spark_not_cached    38.069            29.17
    ## 3          local_R    240.95            3.202
