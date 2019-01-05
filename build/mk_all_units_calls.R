### keep all rescue data and build data for prediction for only rescues.
library('data.table')
library('stringr')
library('lubridate')
setwd("C:/Users/jakek/Documents/intrepidinsight/culverfd")

## this data is created in mk_first_arriving prior to the drops to call-unit uniqueness.
load("data/before_drops_allunits.RData")

# create in of calls with missing times.
final_merged<-final_merged[, missing_something:=is.na(DISPATCH_datetime) | is.na(ENROUTE_datetime) | is.na(ONSCENE_datetime)]

## count and drop all calls with missing times.
nrow(final_merged[missing_something==TRUE])
final_merged<-final_merged[missing_something==FALSE,]

# we now have no obs missing respone times.
nrow(final_merged[is.na(turnout_resptime) | is.na(travel_resptime) | is.na(total_resptime)])
stopifnot(nrow(final_merged[is.na(turnout_resptime) | is.na(travel_resptime) | is.na(total_resptime)])==0)

## Daniel edits

# 1) Filter out calls that are beyond 3 SD from the mean

final_merged2 <- copy(final_merged[final_merged$alarmhandling_resptime >= 0 & final_merged$alarmhandling_resptime <= 314, ])
nrow(final_merged2) #6364

final_merged2 <- final_merged2[final_merged2$turnout_resptime >= 0 & final_merged2$turnout_resptime <= 207, ] 
nrow(final_merged2) #6304

final_merged2 <- final_merged2[final_merged2$travel_resptime >= 0 & final_merged2$travel_resptime <= 568, ] 
nrow(final_merged2) #6209

final_merged2 <- final_merged2[final_merged2$total_resptime >= 0 & final_merged2$total_resptime <= 847, ] 
nrow(final_merged2) #6203


# 2) Filter out NA calls

NA_list <- c("DRILL", "FINVF", "FTEST", "PBASTF", "PDASTF", "STRIKF")

'%!in%' <- function(x,y)!('%in%'(x,y))
sum(final_merged$CALL_TYPE_FINAL_D %in% NA_list) # 0, no cases 


# 3) Filter out mutual aid calls
keep_set <- c("CU11", "CU21", "CU23", "CU33")
nrow(final_merged2) # =6203

final_merged3 <- copy(final_merged2[final_merged2$REP_DIST %in% keep_set,])
nrow(final_merged3) #5962

final_merged<-final_merged3

# check for uniqueness by unit id and callkey
stopifnot(uniqueN(final_merged[, c("CALLKEY", "UNIT_ID")])==nrow(final_merged))

# jake edits
# 4) add on the categories, then limit to relevant categories.
cats_relevant<-data.table(read.csv("raw/20181030_category_types.csv"))
setnames(cats_relevant, c("CALL_TYPE_FINAL", "category_group"))
setkey(final_merged, UNIT_ID, CALLKEY, CALL_TYPE_FINAL)
setkey(cats_relevant,CALL_TYPE_FINAL)
final_merged <- merge(final_merged,cats_relevant, all.x=TRUE)
nrow(final_merged[is.na(category_group)])
#remove 138 na types.
final_merged<-final_merged[!is.na(category_group),]
stopifnot(all(!is.na(final_merged$category_group)))

# under construction:
#5) Add on the fire and rescue districts. We need these to assign calls in our -what-if scenario and do breakdowns.
# find districts using shapefiles
#fire_rescue_districts <- readOGR(dsn = "raw/shapefiles/FDShapefiles", layer = "FireEmsDistricts_Quadrants") 
#grid <- readOGR(dsn = "raw/shapefiles/FDShapefiles", layer = "FireMapBookGrid") 

#dat <- data.frame(x = as.numeric(first_arriving$XCOORD), 
#y =as.numeric(first_arriving$YCOORD))
#sp_dat<-SpatialPointsDataFrame(coords=dat, data=dat)
#proj4string(sp_dat) <- proj4string(fire_rescue_districts)

# 6) add shift sequence and station numbers
final_merged[station_address=="9600 Culver Blvd, Culver City, CA 90232", station_num:="1"]
final_merged[station_address=="11252 Washington Blvd, Culver City, CA 90230", station_num:="2"]
final_merged[station_address=="6030 Bristol Pkwy, Culver City, CA 90230", station_num:="3"]
stopifnot(all(final_merged$station_address!=""))
stopifnot(all(!is.na(final_merged$station_num)))
final_merged[, date:=date(RECEIVED_datetime)]
setkey(final_merged, date, CALLKEY)
crosswalk<-data.table(unique(date(final_merged$date)))
setnames(crosswalk, "date")
crosswalk$shift_letter<-c("A","A", "B", "B", "C", "C")
final_merged<-merge(final_merged, crosswalk, all.x=TRUE, by="date")
final_merged[order(date, CALLKEY)]
all_units_calls<-final_merged

############## Add more features
## 1) Weather
weather_raw<-pdf_text("raw/20181117_weather/1543188_noaa.pdf")
weather2<-unlist(strsplit(weather_raw, "\n"))
# keep only elements that start with 2018 or 2017.
weather3<-weather2[(str_detect(weather2, "2018") | str_detect(weather2, "2017")) & !str_detect(weather2, "[a-zA-Z]{2}")]
weather3 <- data.table(trimws(gsub("\\s+", " ", weather3)))
# date
weather3[, date:=date(str_replace_all(substring(V1,1,11), " ", "-"))]
# max temp
weather3[, max_temp:=as.numeric(word(V1, 4))]
weather3[, min_temp:=as.numeric(word(V1, 5))]
weather3[, atob_temp:=as.numeric(word(V1, 6))]
weather3[, precip:=as.numeric(word(V1, 7))]
weather3<-weather3[, c("date", "max_temp", "min_temp","atob_temp", "precip")]

setkey(weather3, date)
all_units_calls<-merge(all_units_calls, weather3, all.x=TRUE, by="date")

# there are times when the station did not record data. fill these in with santa monica weather.
weather_rawsm<-pdf_text("raw/20181120_sm-weather/sm_noaa.pdf")
weather_sm2<-unlist(strsplit(weather_rawsm, "\n"))
# keep only elements that start with 2018 or 2017.
weather_sm3<-weather_sm2[(str_detect(weather_sm2, "2018") | str_detect(weather_sm2, "2017")) & !str_detect(weather_sm2, "[a-zA-Z]{2}")]
weather_sm3 <- data.table(trimws(gsub("\\s+", " ", weather_sm3)))
# date
weather_sm3[, date:=date(str_replace_all(substring(V1,1,11), " ", "-"))]
# max temp
weather_sm3[, sm_max_temp:=as.numeric(word(V1, 4))]
weather_sm3[, sm_min_temp:=as.numeric(word(V1, 5))]
weather_sm3[, sm_atob_temp:=as.numeric(word(V1, 6))]
weather_sm3[, sm_precip:=as.numeric(word(V1, 7))]
weather_sm3<-weather_sm3[, c("date", "sm_max_temp", "sm_min_temp","sm_atob_temp", "sm_precip")]
setkey(weather_sm3, date)
all_units_calls<-merge(all_units_calls, weather_sm3, all.x=TRUE, by="date")

all_units_calls<-all_units_calls[is.na(max_temp) | max_temp==0, max_temp:=sm_max_temp][is.na(min_temp), min_temp:=sm_min_temp][is.na(precip), precip:=sm_precip][is.na(atob_temp), atob_temp:=sm_atob_temp]

stopifnot(all(all_units_calls$max_temp!=0 ))
stopifnot(any(!is.na(all_units_calls$max_temp) ))

# there are some precipitation obs that are NA (2017-12-20 and 2018-02-27). checked https://www.timeanddate.com/weather/usa/culver-city/historic?month=12&year=2017 to confirm no precipitation.
all_units_calls<-all_units_calls[is.na(precip), precip:=0]
stopifnot(any(!is.na(all_units_calls$precip) ))

## 2) Distance Driving
load("data/drive_distance_final.RData")
setkey(distance_data, start_address_alt, end_address)
all_units_calls<-merge(all_units_calls, distance_data, all.x=TRUE, by=c("start_address_alt", "end_address"))
# max minimum distance not zero.
all_units_calls<-all_units_calls[, clean_drive_distance:=pmax(min(all_units_calls[drive_distance!=0,drive_distance]), drive_distance)]

# google is unable to get one distance, so we look it up manually:
all_units_calls[end_address=="3902 SPAD PL CULVER CITY, CA" & start_address_alt=="11252 Washington Blvd, Culver City, CA 90230", drive_distance:=1230]
stopifnot(nrow(all_units_calls[is.na(drive_distance)])==0)
# there are cases when a call occured right outside the fire station. in these cases, we will add the lowest non-0 distance.
print(min(all_units_calls[drive_distance!=0,drive_distance]))
all_units_calls<-all_units_calls[, clean_drive_distance:=pmax(min(all_units_calls[drive_distance!=0,drive_distance]), drive_distance)]

## 3) Days, month, etc
all_units_calls[, hourofday:= format(RECEIVED_datetime, "%H")][, dofweek:=wday(RECEIVED_DATE, label=TRUE) ][, dofyear:= format(RECEIVED_datetime, "%m-%d")][, month:= format(RECEIVED_datetime, "%m")]

## 4) calls per day
calls_per_day<-unique(all_units_calls[,c("CALLKEY", "date")])
calls_per_day[,daily_calls:=.N, by=date]
calls_per_day<-unique(calls_per_day[,c("daily_calls", "date")])
all_units_calls<-merge(all_units_calls, calls_per_day, all.x=TRUE, by=c("date"))

## 5. check that there are no 0 travel times.
stopifnot(nrow(all_units_calls[travel_resptime==0])==0)

# 6. holidays
holidays<-data.table(read.csv("raw/usholidays.csv"))
holidays[, date:=as.Date.factor(Date)][, Date:=NULL][, X:=NULL]
all_units_calls<-merge(all_units_calls, holidays, all.x=TRUE, by=c("date"))
all_units_calls[, is_fed_holiday:=!is.na(Holiday)]
cor(all_units_calls[, c("travel_resptime", "max_temp", "min_temp", "precip", "drive_distance", "daily_calls")])

save(all_units_calls, file="data/all_units_calls.RData")

