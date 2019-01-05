setwd("C:/Users/jakek/Documents/intrepidinsight/culverfd")
library('readxl')
## Emergency Reporting DF
RMS <- read.csv("raw/EmergencyReportingExport.csv")


## Cars DF1
calls1 <- read.table("raw/FCARSOWNER_FCARSCALL.txt", header = TRUE, sep = "|", fill = TRUE, skip = 1, blank.lines.skip = TRUE, na.strings = c("", "NA"))
calls1 <- calls1[,-1] # Remove first column

na_ind1 <- apply(calls1, MARGIN = 1, function(x) all(is.na(x))) # returns true if row is all NA's
calls1 <- calls1[!na_ind1,] # keeps non-NA rows

dim(calls1) #375  63
# calls1.1 is a dataset with 375 rows
# For validation purposes, check to see if CALLKEY is a unique identifier
length(unique(calls1$CALLKEY)) # 375


## Cars DF2
unit1 <- read.table("raw/FCARSOWNER_FCARSCALLUNIT.txt", header = TRUE, sep = "|", fill = TRUE, skip = 1, blank.lines.skip = TRUE, na.strings = c("", "NA"))
unit1 <- unit1[,-1] # Remove first column

na_ind2 <- apply(unit1, MARGIN = 1, function(x) all(is.na(x))) # returns true if row is all NA's
unit1 <- unit1[!na_ind2,] # keeps non-NA rows

dim(unit1) #1672 29
# unit1 is a dataset with 1672 rows

length(unique(unit1$CALLKEY)) # 699
length(unique(unit1$CALL_NO)) # 699


###############
## Using excel data

calls2 <- read_excel("raw/20180817_20172018_fulldata/FCARSOWNER_FCARSCALL2017-18.xlsx")
unit2 <- read_excel("raw/20180817_20172018_fulldata/FCARSOWNER_FCARSCALLUNIT2017-18.xlsx")

calls_key <- unique(calls2$CALLKEY) 
units_key <- unique(unit2$CALLKEY) 
rms_key <- unique(RMS$Run.Number)

length(calls_key) #7768
length(units_key) #14212
length(rms_key) #7503

# Let's begin by ONLY working with calls_key and units_key
# 1) Remove all observations in unit2 in which UNIT_ID is dispatched more than once to same CALLKEY
ind <- duplicated(unit2[, c(1,3)])
dups <- which(ind == T)

# Testing duplicated outputs
prevs <- dups - 1
unit2[139:140, c(1:3)]

# Removing duplicated observations
unit3 <- unit2[-dups,]
length(unique(unit3$CALLKEY)) # still 142212

ind3 <- duplicated(unit3[, c(1,3)])
sum(ind3) #0


# 2) Remove all observations in unit3 in which UNIT_ID number does not begin with a 4
# Retain only numeric characters from UNIT_ID
unit3$NUM_UNIT_ID <- gsub("[^0-9]", "", unit3$UNIT_ID)

unit3$starts_with_4 <- startsWith(unit3$NUM_UNIT_ID, "4")
##View(cbind(unit3$UNIT_ID, unit3$NUM_UNIT_ID, unit3$starts_with_4))

unit4 <- unit3[unit3$starts_with_4 == TRUE,]


# 3) Remove all observations in calls2 in which REPORT_NO is NA
calls2$is_na <- is.na(calls2$REPORT_NO)
#View(cbind(calls2$REPORT_NO, calls2$is_na))

calls3 <- calls2[calls2$is_na == FALSE,]


# 4) Limiting CAD type to c(E, R, T, BC)
unit4$CHAR_UNIT_ID <- gsub("[^A-z]", "", unit4$UNIT_ID)
#View(cbind(unit4$UNIT_ID, unit4$CHAR_UNIT_ID))
table(unit4$CHAR_UNIT_ID)

# A   AM   BC    E  EMS   FM   FP    L    R    T   TO 
#2674    1  216 7429    2    1   22    1 5763  820    1 


CAD_keep <- c("E", "R", "T", "BC")
check1 <- unit4$CHAR_UNIT_ID %in% CAD_keep
##View(cbind(unit4$CHAR_UNIT_ID, check1))

unit4$CAD_good <- unit4$CHAR_UNIT_ID %in% CAD_keep

unit5 <- unit4[unit4$CAD_good == TRUE,]
table(unit5$CHAR_UNIT_ID)

# Need to remove remaining test calls ----> "FS---------" 
unit5 <- unit5[unit5$CALLKEY != "FS---------",]

# After cleaning, discrepency of 9 unique CALLKEYS
length(unique(unit5$CALLKEY)) #6893
length(unique(calls3$CALLKEY)) #6884


unit_callkeys <- unique(unit5$CALLKEY)
calls_callkeys <- unique(calls3$CALLKEY)

test1 <- unit_callkeys %in% calls_callkeys
sum(test1 == FALSE)

# CALLKEYS in final unit dataset that don't exist in final calls dataset
only_in_unit <- setdiff(unit_callkeys, calls_callkeys)
only_in_unit
# [1] "FS173510045" "FS172320066" "FS172360090" "FS172360091" "FS173250019" "FS173310079" "FS173210038" "FS173340015"
# [9] "FS172590030" "FS172360093" "FS172620025" "FS172620026" "FS172620046" "FS181160032" "FS181320059" "FS181330035"
# [17] "FS181260033" "FS181340030" "FS181190037" "FS180670035" "FS181010065" "FS180260080" "FS180290044" "FS180940048"
# [25] "FS180670078" "FS180670083" "FS180890022" "FS180690053" "FS180320042" "FS180430041" "FS180320025" "FS181520034"
# [33] "FS181410049"

extras_in_unit5 <- unit5[unit5$CALLKEY %in% only_in_unit,]
##View(extras_in_unit5)


# CALLKEYS in final calls dataset that don't exist in final unit dataset
only_in_calls <- setdiff(calls_callkeys, unit_callkeys)
only_in_calls
# [1] "t" "FS171850109" "FS171940083" "FS173330094" "FS173330095" "FS173330096" "FS173330097" "FS173530064"
# [9] "FS180080021" "FS173330085" "FS173330087" "FS173330089" "FS173330090" "FS173330091" "FS173330092" "FS173330093"
# [17] "FS172780087" "FS172600068" "FS172540049" "FS180550054" "FS181260057" "FS180780049" "FS181250009" "FS181580058"

extras_in_calls3 <- calls3[calls3$CALLKEY %in% only_in_calls,]
##View(extras_in_calls3)

## Testing out the extras in calls3 with original unit2
u2 <- unique(unit2$CALLKEY) 
c2 <- unique(calls2$CALLKEY)
call3_u2_match <- only_in_calls[only_in_calls %in% u2] # extra CALLKEYS originally found in unit2 dataset
unit5_c2_match <- only_in_unit[only_in_unit %in% c2] # extra CALLKEYS originally found in calls2 dataset


# Observations that should be deleted from calls3 leftovers
##View(unit2[unit2$CALLKEY %in% call3_u2_match,]) # 4 of them c("FS171850109" "FS173530064" "FS180550054" "FS181580058") have invalid UNIT_ID's
toremove1<-unit2[unit2$CALLKEY %in% call3_u2_match,]
toremove1<-toremove1[ !(toremove1$CALLKEY %in% CAD_keep),]
toremove1<-toremove1$CALLKEY


# Observations that should be deleted from unit5 leftovers
##View(calls2[calls2$CALLKEY %in% unit5_c2_match,])  # ---> All have NA's under REPORT_NO
toremove2<-calls2[calls2$CALLKEY %in% unit5_c2_match,]
toremove2<-toremove2$CALLKEY

# After removing calls3 leftovers with invalid UNIT_ID's
not_in_FCARSCALLUNIT <- extras_in_calls3[(extras_in_calls3$CALLKEY %in% call3_u2_match) == FALSE,]

##write.csv(not_in_FCARSCALLUNIT, file = "not_in_FCARSCALLUNIT.csv")


#### jake additions 20180910 - merge then apply deletions
library('data.table')

unit5<-data.table(unit5)
calls3<-data.table(calls3)

setkey(unit5,CALLKEY)
setkey(calls3,CALLKEY)
length(unique(unit5$CALLKEY))
length(unique(calls3$CALLKEY))
together<-merge(calls3,unit5, all=TRUE)
length(unique(together$CALLKEY))

test<-copy(together[!(CALLKEY %in% toremove1),])
test<-test[!(CALLKEY %in% toremove2),]

## final merge
final_merged<-merge(calls3,unit5)

# check is that difference should be just 20 records.

stopifnot(length(setdiff(unique(test$CALLKEY), unique(final_merged$CALLKEY)))==20)

save(final_merged, file="data/calls_units_merged.RData")

