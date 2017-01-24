library(tidyverse)


all_results <- read_csv("performance.csv") %>%
  mutate(filesize = filesize/1024,
         gb_per_sec = filesize / data_load)

ggplot(data=all_results, aes(x=filesize, y =data_wrangle+data_load, color=category)) +
  geom_line() +
  geom_point() +
  labs(x="File Size (GB)", y="Data Load + Data Wrangle", title="Load+Wrangle Times")

all_results <- filter(all_results, category!="spark_not_cached")

ggplot(data=all_results, aes(x=filesize, y =gb_per_sec, color=category)) +
  geom_line() +
  geom_point() +
  labs(x="File Size (GB)", y="Gigabytes per second", title="Gigabytes per second to load")


ggplot(data=all_results, aes(x=filesize, y =data_load, color=category)) +
  geom_line() +
  geom_point()+
  labs(x="File Size (GB)", y="Seconds", title="Data Load times")

ggplot(data=all_results, aes(x=filesize, y =data_wrangle, color=category)) +
  geom_line() +
  geom_point() +
  labs(x="File Size (GB)", y="Seconds", title="Data Wrangle times")




