library('data.table')
library('stringr')
library('lubridate')
#library(maptools)
#library(rgdal)
#library('rgeos')
setwd("C:/Users/jakek/Documents/intrepidinsight/culverfd")

load("data/calls_units_merged.RData")


## perform tests on some variables to see if they vary at all.
stopifnot(unique(final_merged$ALARM_LEVEL)==1)
stopifnot(unique(final_merged$AGENCY_TYPE)=="F")
stopifnot(unique(final_merged$AGENCY)=="S")
stopifnot(unique(final_merged$JURISDICTION)=="CF")
stopifnot(unique(final_merged$CURR_DAREA)=="F4")
stopifnot(is.na(unique(final_merged$IN_PROGRESS)))
stopifnot(is.na(unique(final_merged$ON_VIEW)))
stopifnot(unique(final_merged$OUT_OF_SERVICE)=="0")
stopifnot(is.na(unique(final_merged$CATCHUP)))

# callno was in both data sets: should be identical
stopifnot(all(final_merged$CALL_NO.x==final_merged$CALL_NO.y))

### generate date times
for (x in c("RECEIVED","ENTRY", "DISPATCH_1ST", "ENROUTE_1ST", "ONSCENE_1ST", "OK_1ST", "CLOSE", "DISPATCH", "ENROUTE", "ONSCENE", "OK", "BEG_TRANSPORT", "END_TRANSPORT", "CLEAR")){
  final_merged<-final_merged[,paste0(x, "_datetime"):= strptime(paste(as.character(get(paste0(x,"_DATE"))), get(paste0(x,"_TIME"))), tz="", format="%Y-%m-%d %H:%M:%S")]
  
}

#### note: remove cancelled calls without enroute time
final_merged<-final_merged[!((is.na(ENROUTE_1ST_datetime) | is.na(ONSCENE_1ST_datetime)) &   DISPOSITION=="CANC") ]

# note that this leaves some cancalled calls:
table(final_merged$DISPOSITION)

# but gets rid of all calls missing enroute time
nrow(final_merged[is.na(ENROUTE_1ST_datetime)])

final_merged<-final_merged[,alarmhandling_resptime:= as.numeric(DISPATCH_datetime-RECEIVED_datetime)][,turnout_resptime:= as.numeric(ENROUTE_datetime-DISPATCH_datetime)][,travel_resptime:= as.numeric(ONSCENE_datetime-ENROUTE_datetime)][,total_resptime:= as.numeric(ONSCENE_datetime-RECEIVED_datetime)]

# is the ordering unique?
stopifnot(nrow(unique(final_merged[,c("CALLKEY", "ONSCENE_datetime", "UNIT_ID")]))==nrow(final_merged))

## add station addresses by unit.
final_merged[str_detect(UNIT_ID, "41") | str_detect(UNIT_ID, "44") | str_detect(UNIT_ID, "45") | str_detect(UNIT_ID, "46"), station_address:= "9600 Culver Blvd, Culver City, CA 90232"]
final_merged[str_detect(UNIT_ID, "42"), station_address:= "11252 Washington Blvd, Culver City, CA 90230"]
final_merged[str_detect(UNIT_ID, "43"), station_address:= "6030 Bristol Pkwy, Culver City, CA 90230"]

###create end address
final_merged[, end_address:=LOCATION]

### edit freeway addresses.
# change FY to FWY
final_merged[,end_address:=str_replace_all(end_address, "FY/", "FWY/")]
final_merged[,end_address:=str_replace_all(end_address, " FY$", " FWY")]


## add city to the end of the address
table(final_merged$CITY)
final_merged[CITY=="CUL", end_address:=paste(end_address, "CULVER CITY, CA")]
final_merged[CITY=="LOS", end_address:=paste(end_address, "LOS ANGELES, CA")]
final_merged[CITY=="COU", end_address:=paste(end_address, "LOS ANGELES, CA")]
final_merged[CITY=="ING", end_address:=paste(end_address, "INGLEWOOD, CA")]

### identify situations where the same vehicle goes to two calls within 2 minutes. flag them
final_merged[order(UNIT_ID,DISPATCH_datetime), flag_after:=as.numeric(DISPATCH_datetime)<120+as.numeric(shift(CLEAR_datetime)), by=UNIT_ID]
final_merged[order(UNIT_ID,DISPATCH_datetime), flag_before:=as.numeric(CLEAR_datetime)+120>as.numeric(shift(DISPATCH_datetime, type="lead")), by=UNIT_ID]
nrow(final_merged[flag_after==TRUE])
##View(final_merged[order(UNIT_ID, DISPATCH_datetime), c("UNIT_ID", "DISPATCH_datetime", "CLEAR_datetime", "flag_after", "flag_before", "LOCATION")][flag_after==TRUE | flag_before==TRUE])

### fill in an alternative start address for those which are flagged after, and the station for those that are not.
final_merged[order(UNIT_ID,DISPATCH_datetime), start_address_alt:=shift(end_address), by=UNIT_ID]
final_merged[flag_after==FALSE | is.na(flag_after),start_address_alt:= station_address]
stopifnot(all(final_merged$start_address_alt!=""))
stopifnot(all(!is.na(final_merged$start_address_alt)))

## save out the big data set before drops and first arriving limits.
save(final_merged, file="data/before_drops_allunits.RData")

## isolate the first arriving unit.
setkey(final_merged, "CALLKEY", "ONSCENE_datetime")
setorder(final_merged, "CALLKEY", "ONSCENE_datetime", "UNIT_ID", na.last=TRUE)
first_arriving<-copy(final_merged[, .SD[1], by = CALLKEY])

# list NA values for response times.
first_arriving[is.na(alarmhandling_resptime),c("RECEIVED_datetime", "DISPATCH_datetime")]
first_arriving[is.na(turnout_resptime), c("DISPATCH_datetime", "ENROUTE_datetime")]
first_arriving[is.na(travel_resptime), c("ONSCENE_datetime", "ENROUTE_datetime")]
first_arriving[is.na(total_resptime), c("RECEIVED_datetime", "ONSCENE_datetime")]

##Based on the above, we create an indicator stating whether a unit has the essential times filled in. We then select the first arriving unit as the earliest onscene time among units with all things filled in.

# create ind
final_merged<-final_merged[, missing_something:=is.na(DISPATCH_datetime) | is.na(ENROUTE_datetime) | is.na(ONSCENE_datetime)]

## remake the first arriving data - using the indicator.
setkey(final_merged, "CALLKEY", "ONSCENE_datetime")
setorder(final_merged, "CALLKEY", "missing_something", "ONSCENE_datetime", "UNIT_ID", na.last=TRUE)
first_arriving<-copy(final_merged[, .SD[1], by = CALLKEY])

# we still have many obs missing a responsetime.
first_arriving[is.na(turnout_resptime), c("ENROUTE_datetime", "ONSCENE_datetime")]
first_arriving[is.na(travel_resptime), c("ONSCENE_datetime", "ENROUTE_datetime")]
first_arriving[is.na(total_resptime), c("RECEIVED_datetime", "ONSCENE_datetime")]

# count them
nrow(first_arriving[is.na(turnout_resptime) | is.na(travel_resptime) | is.na(total_resptime)])

# drop these for now. not trivial - represent 3% ish of data
first_arriving<-first_arriving[missing_something==0]
stopifnot(nrow(first_arriving[is.na(turnout_resptime) | is.na(travel_resptime) | is.na(total_resptime)])==0)

resp_types<-c("alarmhandling_resptime", "turnout_resptime", "travel_resptime", "total_resptime")

# test out some summary stats, but don't save them.
summary_stats<-lapply(first_arriving[, c("alarmhandling_resptime", "turnout_resptime", "travel_resptime", "total_resptime")], function(x) rbind(mean=mean(x,na.rm=TRUE), decile1 =quantile(x, probs=0.1, na.rm=TRUE),decile2 =quantile(x, probs=0.2, na.rm=TRUE),                           decile3 =quantile(x, probs=0.3, na.rm=TRUE), decile4 =quantile(x, probs=0.4, na.rm=TRUE), decile5 =quantile(x, probs=0.5, na.rm=TRUE),
                                                                                                                                        decile6 =quantile(x, probs=0.6, na.rm=TRUE), decile7 =quantile(x, probs=0.7, na.rm=TRUE), decile8 =quantile(x, probs=0.8, na.rm=TRUE),
                                                                                                                                                decile9 =quantile(x, probs=0.9, na.rm=TRUE)))
stats_allcalls<-t(data.frame(summary_stats))
rownames(stats_allcalls)<-NULL
stats_allcalls<-data.table(cbind(c("Alarm Handling", "Turnout", "Travel", "Total"), stats_allcalls))
setnames(stats_allcalls, old="V1", new="Type")
stats_allcalls

first_arriving<-first_arriving[,all_data:="OVERALL"]


## Daniel edits

#6483

# 1) Filter out calls that are beyond 3 SD from the mean

first_arriving2 <- copy(first_arriving[first_arriving$alarmhandling_resptime >= 0 & first_arriving$alarmhandling_resptime <= 314, ])
nrow(first_arriving2) #6364

first_arriving2 <- first_arriving2[first_arriving2$turnout_resptime >= 0 & first_arriving2$turnout_resptime <= 207, ] 
nrow(first_arriving2) #6304

first_arriving2 <- first_arriving2[first_arriving2$travel_resptime >= 0 & first_arriving2$travel_resptime <= 568, ] 
nrow(first_arriving2) #6209

first_arriving2 <- first_arriving2[first_arriving2$total_resptime >= 0 & first_arriving2$total_resptime <= 847, ] 
nrow(first_arriving2) #6203


# 2) Filter out NA calls

NA_list <- c("DRILL", "FINVF", "FTEST", "PBASTF", "PDASTF", "STRIKF")

'%!in%' <- function(x,y)!('%in%'(x,y))
sum(final_merged$CALL_TYPE_FINAL_D %in% NA_list) # 0, no cases 


# 3) Filter out mutual aid calls
keep_set <- c("CU11", "CU21", "CU23", "CU33")
nrow(first_arriving2) # =6203

first_arriving3 <- copy(first_arriving2[first_arriving2$REP_DIST %in% keep_set,])
nrow(first_arriving3) #5962

first_arriving<-first_arriving3

# jake edits
# 4) add on the categories, then limit to relevant categories.
cats_relevant<-data.table(read.csv("raw/20181030_category_types.csv"))
setnames(cats_relevant, c("CALL_TYPE_FINAL", "category_group"))
setkey(first_arriving, CALLKEY, CALL_TYPE_FINAL)
setkey(cats_relevant,CALL_TYPE_FINAL)
first_arriving <- merge(first_arriving,cats_relevant, all.x=TRUE)
nrow(first_arriving[is.na(category_group)])
#remove 138 na types.
first_arriving<-first_arriving[!is.na(category_group),]
stopifnot(all(!is.na(first_arriving$category_group)))

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
first_arriving[station_address=="9600 Culver Blvd, Culver City, CA 90232", station_num:="1"]
first_arriving[station_address=="11252 Washington Blvd, Culver City, CA 90230", station_num:="2"]
first_arriving[station_address=="6030 Bristol Pkwy, Culver City, CA 90230", station_num:="3"]
stopifnot(all(first_arriving$station_address!=""))
stopifnot(all(!is.na(first_arriving$station_num)))
first_arriving[, date:=date(RECEIVED_datetime)]
setkey(first_arriving, date, CALLKEY)
crosswalk<-data.table(unique(date(first_arriving$date)))
setnames(crosswalk, "date")
crosswalk$shift_letter<-c("A","A", "B", "B", "C", "C")
first_arriving<-merge(first_arriving, crosswalk, all.x=TRUE, by="date")
first_arriving[order(date, CALLKEY)]

save(first_arriving, file="data/first_arriving.RData")
