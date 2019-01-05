library('data.table')
library('stringr')
library('lubridate')

setwd("C:/Users/jakek/Documents/intrepidinsight/culverfd")

load("data/calls_units_merged.RData")

### generate date times
for (x in c("RECEIVED","ENTRY", "DISPATCH_1ST", "ENROUTE_1ST", "ONSCENE_1ST", "OK_1ST", "CLOSE", "DISPATCH", "ENROUTE", "ONSCENE", "OK", "BEG_TRANSPORT", "END_TRANSPORT", "CLEAR")){
  final_merged<-final_merged[,paste0(x, "_datetime"):= strptime(paste(as.character(get(paste0(x,"_DATE"))), get(paste0(x,"_TIME"))), tz="", format="%Y-%m-%d %H:%M:%S")]
  
}

#### note: remove cancelled calls without enroute time
final_merged<-final_merged[!((is.na(ENROUTE_1ST_datetime) | is.na(ONSCENE_1ST_datetime)) &   DISPOSITION=="CANC") ]

# check how sensitive the count is to the cushion value.
for (x in seq(0,1200, by=30)){
  final_merged[order(DISPATCH_datetime), flag_after:=as.numeric(DISPATCH_datetime)<x+as.numeric(shift(CLEAR_datetime)), by=UNIT_ID]
  count=nrow(final_merged[flag_after==TRUE])
  print(paste(x, ":", count))
}

final_merged[order(UNIT_ID,DISPATCH_datetime), flag_after:=as.numeric(DISPATCH_datetime)<120+as.numeric(shift(CLEAR_datetime)), by=UNIT_ID]
final_merged[order(UNIT_ID,DISPATCH_datetime), flag_before:=as.numeric(CLEAR_datetime)+120>as.numeric(shift(DISPATCH_datetime, type="lead")), by=UNIT_ID]
nrow(final_merged[flag_after==TRUE])
View(final_merged[order(UNIT_ID, DISPATCH_datetime), c("UNIT_ID", "DISPATCH_datetime", "CLEAR_datetime", "flag_after", "flag_before", "LOCATION")][flag_after==TRUE | flag_before==TRUE])





