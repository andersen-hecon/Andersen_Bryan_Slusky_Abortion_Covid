library(tidyverse)
library(collapse)

# go get the normalization stats data
s_dates<-seq(lubridate::as_date("2018-01-01"),to=lubridate::as_date("2021-01-04"),by=7)%>%
  str_replace_all("-","_")

home_panel_summaries<-
  map_chr(s_dates,function(d) glue::glue("../SafeGraph/cleaned/weekly/release-2021-07/home_panel_summary_{d}.csv.gz"))%>%
  read_csv()

normalization_stats<-
  map_chr(s_dates,function(d) glue::glue("../SafeGraph/cleaned/weekly/release-2021-07/normalization_{d}.csv.gz"))%>%
  read_csv()

normalization_stats<-
  normalization_stats%>%
  filter(iso_country_code=="US")%>%
  mutate(date=lubridate::make_date(year=year,month=month,day=day),
         region=str_to_upper(region))%>%
  inner_join(tigris::fips_codes%>%distinct(region=state,state_fips=state_code))%>%
  filter(state_fips<57)%>%
  mutate(eweek=lubridate::epiweek(date),
         eday=lubridate::wday(date),
         eyear=lubridate::epiyear(date))

normalization_stats%>%
  select(state_fips,date,eyear,eweek,eday,total_visits,total_devices_seen)%>%
  mutate(state_fips=as.numeric(state_fips))%>%
  write_csv('./Data/consol_normalization.csv.gz')

pop_data<-
  read_csv("https://static.usafacts.org/public/data/covid-19/covid_county_population_usafacts.csv")

pop_data<-
  pop_data%>%
  select(county_fips=countyFIPS,State,pop=population)

pop_data<-
  pop_data%>%
  inner_join(tigris::fips_codes%>%distinct(State=state,state_fips=state_code))%>%
  mutate(state_fips=as.numeric(state_fips))

usafacts_data<-
  read_csv("https://static.usafacts.org/public/data/covid-19/covid_confirmed_usafacts.csv")

usafacts_data<-
  usafacts_data%>%
  select(-`County Name`)%>%
  rename(county_fips=countyFIPS,state_fips=StateFIPS)%>%
  filter(state_fips<57)%>%
  mutate(state_fips=as.numeric(state_fips))%>%
  pivot_longer(-c(State,county_fips,state_fips),names_to="date",values_to="cum_cases")%>%
  mutate(date=lubridate::as_date(date))%>%
  inner_join(pop_data)

# create state aggregates
usafacts_data<-
  usafacts_data%>%
  filter(county_fips!=0)%>%
  bind_rows(
    usafacts_data%>%
      group_by(state_fips,date)%>%
      summarize(across(c(cum_cases,pop),~sum(.)),.groups="drop")
    )

# extract pop_data from usafacts_data
pop_data<-
  usafacts_data%>%
  distinct(state_fips,county_fips,pop)

# convert to incidence per 100,000 and create state aggregates
usafacts_data<-
  usafacts_data%>%
  arrange(state_fips,county_fips,date)%>%
  mutate(new_cases=cum_cases-lag(cum_cases,default=0),
         new7_cases=(cum_cases-lag(cum_cases,n=7,default=0))/7,
         across(c(cum_cases,new_cases,new7_cases),~./(pop/100000),.names="{col}_per100k"))%>%
  ungroup()%>%
  select(county_fips,state_fips,date,ends_with("per100k"))

usafacts_data2<-
  usafacts_data%>%
  # group_by(state_fips,date)%>%
  # arrange(state_fips,date,county_fips)%>%
  # mutate(across(ends_with("per100k"),~first(.),.names="state_{col}"))%>%
  # ungroup()%>%
  filter(county_fips==0 | is.na(county_fips))%>%
  select(-county_fips)

clinics<-
  read_csv("./Data/ansirh_matched_core_2.csv")

clinics<-
  clinics%>%
  filter(!`Facility Name` %in% c("Planned Parenthood of Greater Texas: Southwest Fort Worth Health Center",
                                 "Brooklyn Women's Pavilion OB/GYN PLLC: Brooklyn",
                                 "Planned Parenthood of Western Pennsylvania: Pittsburgh Family Planning Health Center"))%>%
  filter(naics_code %in% c(621111,
                           621410,
                           622310))%>%
  inner_join(distinct(tigris::fips_codes,state,state_fips=state_code),by=c("region"="state"))

clinics<-
  clinics%>%
  mutate(abortion           =str_detect(`Provides Abortions`,"^Provides"),
         medication_abortion=str_detect(MAB,"Yes"),
         surgical_abortion  =str_detect(`Surgical/aspiration`,"Yes"),
         across(abortion:surgical_abortion,~if_else(.,1,0,missing=0)),
         medication_abortion=medication_abortion*abortion,
         surgical_abortion=surgical_abortion*abortion,
         state_fips=as.numeric(state_fips)
  )


clinics<-
  clinics%>%
  select(placekey,naics_code,abortion,medication_abortion,surgical_abortion,region,state_fips)

clinics<-
  clinics%>%
  filter(abortion==1)

#
# Use Core and spatial to get county fips codes
#
core<-read_csv("/proj/msander4lab/projects/SafeGraph/cleaned/core/2021_07_07_core.csv.gz")
counties<-tigris::counties()

core<-
  core%>%
  inner_join(select(clinics,placekey))%>%
  select(placekey,latitude,longitude)

core<-
  core%>%
  sf::st_as_sf(coords=c("longitude","latitude"),crs=sf::st_crs(counties))%>%
  sf::st_join(select(counties,county_fips=GEOID))%>%
  mutate(county_fips=as.numeric(county_fips))

clinics_county_pop<-
  core%>%
  inner_join(pop_data)%>%
  as_tibble()%>%
  select(placekey,county_fips,pop)

# monthly pivoted data
visits_by_day<-read_csv("./Data/daily_visits.csv")%>%
  select(placekey,date,visitors)

# interpolate the monthly data
visits_by_day<-
  visits_by_day%>%
  expand(placekey,date)%>%
  inner_join(
    visits_by_day%>%
      group_by(placekey)%>%
      summarize(min_date=min(date),
                max_date=max(date))%>%
      ungroup(),
    by="placekey")%>%
  filter(date>=min_date,date<=max_date)%>%
  select(-min_date,-max_date)%>%
  left_join(visits_by_day,by=c("placekey","date"))%>%
  mutate(visitors=replace_na(visitors,0))

# weekly data
weekly_patterns_stats<-read_csv("./Data/weekly_patterns_stats.csv")

weekly_patterns_stats<-
  weekly_patterns_stats%>%
  expand(placekey,date)%>%
  inner_join(
    weekly_patterns_stats%>%
      group_by(placekey)%>%
      summarize(min_date=min(date),
                max_date=max(date))%>%
      ungroup(),
    by="placekey")%>%
  filter(date>=min_date,date<=max_date)%>%
  select(-min_date,-max_date)%>%
  left_join(weekly_patterns_stats,by=c("placekey","date"))%>%
  mutate(across(c(raw_visit_counts,raw_visitor_counts,median_dwell) | contains("visitors"),~replace_na(.,0)))

weekly_patterns_stats<-
  weekly_patterns_stats%>%
  distinct(date)%>%
  inner_join(select(visits_by_day,placekey,date))%>%
  left_join(weekly_patterns_stats)%>%
  mutate(across(c(raw_visit_counts,raw_visitor_counts) | starts_with("visitors_dur"),~replace_na(.,0)),
         visitors_dur_1h=`visitors_dur<5`+`visitors_dur5-10`+`visitors_dur11-20`+`visitors_dur21-60`,
         visitors_dur_1_4h=`visitors_dur61-120`+`visitors_dur121-240`,
         visitors_dur_4h=`visitors_dur>240`)%>%
  select(-(`visitors_dur<5`:`visitors_dur>240`))

weekly_tract_visits<-read_csv("./Data/weekly_tract_visits.csv")

# weekly county-level
home_panel_summaries_tract<-
  home_panel_summaries%>%
  filter(str_sub(census_block_group,1,2)<"57")%>%
  mutate(census_tract=str_sub(census_block_group,1,11))%>%
  select(-census_block_group)%>%
  fgroup_by(date_range_start:iso_country_code,census_tract)%>%
  fsum()%>%
  mutate(date=lubridate::as_date(date_range_start))

weekly_county_level<-
  weekly_tract_visits%>%
  inner_join(select(home_panel_summaries_tract,census_tract,number_devices_residing,date),by=c("origin_census_tract"="census_tract","date"))%>%
  mutate(visitor_count=pmin(visitor_count,number_devices_residing))%>%
  group_by(placekey,date,county_fips=str_sub(origin_census_tract,1,5))%>%
  summarise(visitor_count=sum(visitor_count))%>%
  ungroup()%>%
  filter(str_detect(county_fips,"^[0-9]{2}"))%>%
  mutate(county_fips=as.numeric(county_fips))

weekly_tract_visits<-
  weekly_tract_visits%>%
  group_by(placekey,date,state_fips=str_sub(origin_census_tract,1,2))%>%
  summarise(visitor_count=sum(visitor_count))%>%
  ungroup()%>%
  filter(str_detect(state_fips,"[0-9]{2}"))%>%
  mutate(state_fips=as.numeric(state_fips))

weekly_tract_visits<-
  weekly_tract_visits%>%
  expand(placekey,state_fips,date)%>%
  inner_join(
    weekly_tract_visits%>%
      group_by(placekey)%>%
      summarize(min_date=min(date),
                max_date=max(date))%>%
      ungroup(),
    by="placekey")%>%
  filter(date>=min_date,date<=max_date)%>%
  left_join(weekly_tract_visits)%>%
  mutate(visitor_count=replace_na(visitor_count,0))


weekly_tract_visits<-
  weekly_tract_visits%>%
  inner_join(select(clinics,placekey,clinic_state_fips=state_fips))

# clinic level in-state, out-state 
weekly_tract_oos<-
  weekly_tract_visits%>%
  mutate(out_of_state=if_else(clinic_state_fips==state_fips,"in_state","out_state"))%>%
  count(placekey,date,out_of_state,wt=visitor_count)%>%
  pivot_wider(names_from="out_of_state",values_from = "n",values_fill = 0)%>%
  inner_join(select(weekly_patterns_stats,placekey,date,raw_visit_counts))%>%
  select(placekey,date,in_state,out_state)

weekly_data<-
  full_join(
    weekly_patterns_stats,
    weekly_tract_oos
  )%>%
  rename_with(~str_replace(.,"raw_","weekly_"))

# state level in and out data
# of visitors from within the state
# of visitors from outside the state
# of visitors who left the state

weekly_tract_dep<-
  weekly_tract_visits%>%
  group_by(date,clinic_state_fips)%>%
  summarize(within_state_visitors=sum(if_else(state_fips==clinic_state_fips,visitor_count,0)),
            outof_state_visitors=sum(if_else(state_fips!=clinic_state_fips,visitor_count,0)))%>%
  ungroup()%>%
  rename(state_fips=clinic_state_fips)%>%
  full_join(
    weekly_tract_visits%>%
      group_by(date,state_fips)%>%
      summarize(
        left_state_visitors=sum(if_else(state_fips!=clinic_state_fips,visitor_count,0))
      )
    )%>%
  mutate(across(within_state_visitors:left_state_visitors,~replace_na(.,0)))

# policy dates
policy_dates<-
  read_csv("./Data/policy_dates.csv")%>%
  inner_join(distinct(tigris::fips_codes,state_fips=state_code,State=state_name))%>%
  mutate(across(c(-state_fips,-State),~lubridate::mdy(.)))%>%
  inner_join(visits_by_day%>%distinct(date),by=character())%>%
  mutate(eyear=lubridate::epiyear(date),
       eweek=lubridate::epiweek(date),
       dow=lubridate::wday(date),
       year20=if_else(eyear==2020,1,0))%>%
  mutate(elective_ban=case_when(date>=`Elective procedures resume`~0,
                                date>=`Elective procedure ban`~1,
                                TRUE~0),
         stay_at_home=case_when(date>=`Stay-At-Home Order End`~0,
                                date>=`Stay-At-Home Order Start`~1,
                                TRUE~0),
         nonessential=case_when(date>=`Non Essential Services Open`~0,
                                date>=`Non Essential Services Close`~1,
                                TRUE~0),
         stay_at_home_ne=pmax(stay_at_home,nonessential),
         surgical=case_when(state_fips=="01" & date %in% lubridate::as_date("2020-03-27"):lubridate::as_date("2020-04-30") ~ 1,
                            state_fips=="02" & date %in% lubridate::as_date("2020-04-08"):lubridate::as_date("2020-04-30") ~ 1,
                            state_fips=="05" & date %in% lubridate::as_date("2020-04-03"):lubridate::as_date("2020-04-13") ~ 1,
                            state_fips=="05" & date %in% lubridate::as_date("2020-04-22"):lubridate::as_date("2020-04-27") ~ 1,
                            state_fips=="18" & date %in% lubridate::as_date("2020-04-01"):lubridate::as_date("2020-04-27") ~ 1,
                            state_fips=="19" & date %in% lubridate::as_date("2020-03-27"):lubridate::as_date("2020-04-24") ~ 1,
                            state_fips=="22" & date %in% lubridate::as_date("2020-03-21"):lubridate::as_date("2020-05-01") ~ 1,
                            state_fips=="28" & date %in% lubridate::as_date("2020-04-10"):lubridate::as_date("2020-05-11") ~ 1, # this is a modification from our earlier coding.
                            state_fips=="39" & date %in% lubridate::as_date("2020-03-17"):lubridate::as_date("2020-05-01") ~ 1,
                            state_fips=="40" & date %in% lubridate::as_date("2020-03-27"):lubridate::as_date("2020-04-06") ~ 1,
                            state_fips=="46" & date %in% lubridate::as_date("2020-03-13"):lubridate::as_date("2020-10-01") ~ 1,
                            state_fips=="47" & date %in% lubridate::as_date("2020-04-08"):lubridate::as_date("2020-04-17") ~ 1,
                            state_fips=="48" & date %in% lubridate::as_date("2020-03-23"):lubridate::as_date("2020-03-26") ~ 1,
                            state_fips=="48" & date %in% lubridate::as_date("2020-03-31"):lubridate::as_date("2020-04-22") ~ 1,
                            state_fips=="54" & date %in% lubridate::as_date("2020-04-01"):lubridate::as_date("2020-04-30") ~ 1,
                            TRUE ~0),
         surgical2=case_when(state_fips=="01" & date %in% lubridate::as_date("2020-03-28"):lubridate::as_date("2020-04-12") ~ 1,
                             state_fips=="02" & date %in% lubridate::as_date("2020-04-07"):lubridate::as_date("2020-05-04") ~ 1,
                             state_fips=="05" & date %in% lubridate::as_date("2020-04-03"):lubridate::as_date("2020-04-13") ~ 1,
                             state_fips=="05" & date %in% lubridate::as_date("2020-04-22"):lubridate::as_date("2020-05-18") ~ 1, # chose the 72 hour rule for a negative covid test
                             state_fips=="18" & date %in% lubridate::as_date("2020-04-01"):lubridate::as_date("2020-04-27") ~ 0, # treat Indiana as not banning Abortion, since no one oppposed the order
                             state_fips=="19" & date %in% lubridate::as_date("2020-03-27"):lubridate::as_date("2020-04-01") ~ 1, 
                             state_fips=="22" & date %in% lubridate::as_date("2020-03-21"):lubridate::as_date("2020-05-01") ~ 1,
                             state_fips=="28" & date %in% lubridate::as_date("2020-04-10"):lubridate::as_date("2020-05-11") ~ 1,
                             state_fips=="39" & date %in% lubridate::as_date("2020-03-17"):lubridate::as_date("2020-03-30") ~ 1,
                             state_fips=="40" & date %in% lubridate::as_date("2020-03-27"):lubridate::as_date("2020-04-21") ~ 1, # went with the fully effective date of the injunction
                             state_fips=="46" & date %in% lubridate::as_date("2020-03-13"):lubridate::as_date("2020-10-01") ~ 1,
                             state_fips=="47" & date %in% lubridate::as_date("2020-04-08"):lubridate::as_date("2020-04-17") ~ 1,
                             state_fips=="48" & date %in% lubridate::as_date("2020-03-23"):lubridate::as_date("2020-04-22") ~ 1,
                             state_fips=="54" & date %in% lubridate::as_date("2020-04-01"):lubridate::as_date("2020-04-30") ~ 1,
                             TRUE ~0),
         surgical=elective_ban*surgical,
         surgical2=elective_ban*surgical2,
         holiday=case_when(dow==2 & eweek==12 & eyear==2020~1,
                           dow==5 & eweek==15 & eyear==2020~1,
                           dow==5 & eweek==16 & eyear==2019~1,
                           dow==1 & eweek==22 & eyear==2019~1,
                           dow==1 & eweek==22 & eyear==2020~1,
                           dow==1 & eweek==8 & eyear==2019~1,
                           dow==1 & eweek==8 & eyear==2020~1,
                           TRUE~0)
         
  )%>%
  group_by(state_fips,eweek,dow)%>%
  arrange(eyear,.by_group = TRUE)%>%
  mutate(cf_date=last(date))%>%
  ungroup()%>%
  group_by(state_fips)%>%
  mutate(relative_time_elective=case_when(cf_date>=`Elective procedures resume`~NA_real_,
                                          is.na(`Elective procedure ban`)~-1,
                                          TRUE~as.numeric(cf_date-`Elective procedure ban`)),
         relative_time_stayhome=case_when(is.finite(min(if_else(stay_at_home_ne==1,date,lubridate::NA_Date_),na.rm=T))~as.numeric(cf_date-min(if_else(stay_at_home_ne==1,date,lubridate::NA_Date_),na.rm=T)),
                                          TRUE~-1)
                                            )%>%
  select(-(`Elective procedure ban`:`Non Essential Services Open`), -State)
  
policy_dates<-
  policy_dates%>%
  mutate(state_fips=as.numeric(state_fips))

all_sg_data<-
  left_join(visits_by_day,weekly_data)%>%
  inner_join(select(clinics,placekey,state_fips))%>%
  inner_join(clinics_county_pop)%>%
  inner_join(policy_dates)%>%
  left_join(usafacts_data)%>%
  mutate(across(contains("per100k"),~replace_na(.,0)))%>%
  rename_with(~str_replace(.,"<","_"))%>%
  rename_with(~str_replace(.,">","_"))%>%
  rename_with(~str_replace(.,"-","_"))
                                
all_sg_data%>%
  inner_join(clinics)%>%
  write_csv("./Data/combined_analytic.csv")

# create a state-level dataset
state_data<-
  all_sg_data%>%
  select(date,state_fips,distance_from_home,median_dwell,visitors:out_state)%>%
  group_by(state_fips,date)%>%
  summarize(distance_from_home=weighted.mean(distance_from_home,weekly_visitor_counts),
            median_dwell=weighted.mean(median_dwell,weekly_visit_counts),
            across(visitors:out_state,~sum(.)),
            .groups="drop")%>%
  inner_join(policy_dates)%>%
  left_join(weekly_tract_dep)%>%
  left_join(usafacts_data2)%>%
  left_join(filter(pop_data,county_fips==0 | is.na(county_fips))%>%
              select(-county_fips)%>%
              mutate(state_fips=as.numeric(state_fips)))%>%
  mutate(across(contains("per100k"),~replace_na(.,0)))

state_data%>%
  write_csv("./Data/state_analytic.csv")

weekly_county_level_merged<-
  weekly_county_level%>%
  filter(date>="2019-01-01")%>%
  expand(nesting(placekey,county_fips),date)%>%
  left_join(weekly_county_level)%>%
  mutate(visitor_count=replace_na(visitor_count,0))%>%
  inner_join(pop_data)%>%
  left_join(usafacts_data)%>%
  mutate(across(contains("per100k"),~replace_na(.,0)))%>%
  select(-state_fips)%>%
  rename(home_county_fips=county_fips)%>%
  inner_join(select(clinics,placekey,state_fips))%>%
  #inner_join(select(clinics_county_pop,-county_fips))%>%
  inner_join(policy_dates)


weekly_county_level_merged%>%
  inner_join(clinics)%>%
  write_csv("./Data/county_analytic.csv")

home_panel_summaries%>%
  filter(iso_country_code=="US")%>%
  mutate(county_fips=str_sub(census_block_group,1,5),
         date=lubridate::as_date(date_range_start))%>%
  select(date,county_fips,number_devices_residing)%>%
  fgroup_by(date,county_fips)%>%
  fsum()%>%
  write_csv("./Data/county_home_panel_summaries.csv")

