library(tidyverse)
library(multidplyr)

safegraph_root="/proj/msander4lab/projects/SafeGraph"


clinics<-
  read_csv("./Data/ansirh_matched_core_2.csv")

pk_list<-
  clinics%>%
  distinct(placekey)

file_list<-
  list.files(paste0(safegraph_root,"/cleaned/monthly/release-2021-07"),pattern="*_patterns_20[12][098]_*",full.names=TRUE)

cluster<-new_cluster(8)

cluster_library(cluster,c("tidyverse"))

cluster_assign_partition(cluster,files=file_list)
cluster_assign(cluster,pk_list=pk_list)

cluster_call(cluster,
             data<-map_dfr(files,
                     ~read_csv(.,col_select =c(placekey,date,raw_visit_counts,raw_visitor_counts,
                                               starts_with("visits_by_day")))%>%
                       inner_join(pk_list)
             )
)

cl_data<-
  party_df(cluster,"data")
          
monthly_data<-
  cl_data%>%
  collect()

monthly_data%>%
  pivot_longer(cols=starts_with("visits_by_day"),
               names_to="day",
               values_to="visitors")%>%
  mutate(day=as.integer(str_remove(day,"^visits_by_day")),
         date=date+day)%>%
  filter(!is.na(visitors))%>%
  select(-day)%>%
  write_csv("./Data/daily_visits.csv")

rm(cl_data)

cluster_call(cluster,rm(data,files))

file_list<-
  list.files(paste0(safegraph_root,"/cleaned/weekly/release-2021-07"),pattern="*_patterns_20[12][089]_*",full.names=TRUE)

cluster_assign_partition(cluster,files=file_list)

cluster_call(cluster,
             data<-map_dfr(files,
                           ~read_csv(.,
                                     col_select =c(placekey,date,raw_visit_counts,raw_visitor_counts,distance_from_home,median_dwell,starts_with("visitors_dur")))%>%
                             inner_join(pk_list)
             )
)

weekly_patterns<-
  party_df(cluster,"data")%>%
  collect()

weekly_patterns%>%
  write_csv("./Data/weekly_patterns_stats.csv")

rm(weekly_patterns)


file_list=str_replace(file_list,"patterns","tract")

cluster_assign_partition(cluster,files=file_list)

cluster_call(cluster,
             data<-map_dfr(files,
                           function(f) {
                             date=lubridate::as_date(str_extract(f,"[0-9]{4}_[0-9]{2}_[0-9]{2}"))
                             
                             read_csv(f)%>%
                               inner_join(pk_list)%>%
                               mutate(date=date)%>%
                               return()
                           }
             )
)

weekly_tracts<-
  party_df(cluster,"data")%>%
  collect()

weekly_tracts%>%
  write_csv("./Data/weekly_tract_visits.csv")

file_list=str_replace(file_list,"tract","cbgs")
cluster_assign_partition(cluster,files=file_list)

cluster_call(cluster,
             data<-map_dfr(files,
                           function(f) {
                             date=lubridate::as_date(str_extract(f,"[0-9]{4}_[0-9]{2}_[0-9]{2}"))
                             
                             read_csv(f)%>%
                               inner_join(pk_list)%>%
                               mutate(date=date)%>%
                               return()
                           }
             )
)

weekly_cbgs<-
  party_df(cluster,"data")%>%
  collect()

weekly_cbgs%>%
  write_csv("./Data/weekly_cbgs_visits.csv")
