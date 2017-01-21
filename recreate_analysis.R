

library(sparklyr)
library(tidyverse)

conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "16G"
sc <- spark_connect(master="local", config = conf)

spark_read_csv(sc, "reviews" ,"allfiles.csv", header = FALSE, memory=FALSE, repartition = 14) 

df <- tbl(sc,"reviews") %>%
  select(user_id = V1,
         item_id = V2,
         rating = V3,
         timestamp = V4,
         category = V5)

df <- df %>% group_by(user_id, item_id) %>% filter(row_number(item_id) == 1) %>% ungroup()

hours_offset <- 8
df <- df %>% mutate(timestamp_f = from_unixtime(timestamp + hours_offset*60*60))
df <- df %>% mutate(hour = hour(timestamp_f),
                    dayofweek = date_format(timestamp_f, 'EEEE'),
                    month = month(timestamp_f),
                    year = year(timestamp_f))

df <- df %>% group_by(user_id) %>% mutate(user_nth = min_rank(timestamp)) %>% ungroup()
df <- df %>% group_by(item_id) %>% mutate(item_nth = min_rank(timestamp)) %>% ungroup()

df_t <- df %>% sdf_register("data_t")
system.time(tbl_cache(sc, "data_t"))

nrow(tbl(sc, "data_t"))

df_t %>% filter(item_id == "0739048287") %>% collect()


df_agg <- df_t %>%
  group_by(category) %>%
  summarize(count = n(), avg_rating = mean(rating)) %>%
  arrange(desc(avg_rating)) %>%
  collect()
df_agg

df_user_review_counts <- df_t %>%
  group_by(user_id) %>%
  summarize(num_reviews=n()) %>%
  group_by(num_reviews) %>%
  summarize(total=n()) %>%
  arrange(num_reviews) %>%
  collect()
df_user_review_counts %>% head(100)
df_user_review_counts <- df_t %>%
  group_by(user_id) %>%
  summarize(num_reviews=n()) %>%
  group_by(num_reviews) %>%
  summarize(total=n()) %>%
  arrange(num_reviews) %>%
  collect()
df_user_review_counts %>% head(100)
df_temp <- df_user_review_counts %>%
  mutate(norm = total/sum(total), prop = cumsum(norm)) %>%
  filter(num_reviews <= 50)
plot <- ggplot(df_temp, aes(x=num_reviews, y=prop)) +
  geom_line(color="#2980b9") +
  fte_theme() +
  scale_y_continuous(labels = percent, limits=c(0,1)) +
  labs(title="Cumulative Proportion of # Amazon Reviews Given by User", x="# Reviews Given By User", y="Cumulative Proportion of All Amazon Reviewers")
max_save(plot, "user_count_cum", "Amazon")



