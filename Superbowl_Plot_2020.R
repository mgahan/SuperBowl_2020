
# Bring in libraries
library(data.table)
library(dygraphs)

# Extract current files
all_files <- fread(cmd="aws s3 --profile scott ls s3://havas-data-science/Super_Bowl/SB_20/ --recursive", header = FALSE)
all_files <- all_files[V4 %like% ".csv"]
all_files[, Time := substr(V4, nchar(V4)-18, nchar(V4)-4)]
all_files[, Time := as.POSIXct(Time, format="%Y%m%d_%H%M%S",tz="America/Los_Angeles")]
all_files[, Company := basename(dirname(dirname(V4)))]
extra_files <- all_files[Company %in% c("poptarts","alexa")]
all_files1 <- all_files[Time < as.POSIXct("2020-02-02 19:21:00", tz="America/Los_Angeles")]
setorder(all_files1, Company, -Time)
all_files1 <- all_files1[, .SD[1L], by=.(Company)]
all_files2 <- all_files[Time < as.POSIXct("2020-02-02 21:21:00", tz="America/Los_Angeles")]
setorder(all_files2, Company, -Time)
all_files2 <- all_files2[, .SD[1L], by=.(Company)]
all_files <- rbindlist(list(all_files1, all_files2), fill=TRUE)
setorder(all_files, Company,Time)
all_files <- all_files[, .SD[1L], by=.(Company)]
all_files <- rbindlist(list(all_files, extra_files), fill=TRUE)
all_files <- all_files[Company != "pop-tarts"]

# One of modifications
all_files[Company=="sylvester_stallone", V4 := "Super_Bowl/SB_20/sylvester_stallone/CSVs/sylvester_stallone_20200202_212514.csv"]
all_files[Company=="audi", V4 := "Super_Bowl/SB_20/audi/CSVs/audi_20200202_210048.csv"]
all_files[Company=="facebook", V4 := "Super_Bowl/SB_20/facebook/CSVs/facebook_20200202_212438.csv"]
all_files[Company=="ellen", V4 := "Super_Bowl/SB_20/ellen/CSVs/ellen_20200202_213741.csv"]

# Earlier 
all_files[Company=="kobe", V4 := "Super_Bowl/SB_20/kobe/CSVs/kobe_20200202_180426.csv"]
all_files[Company=="juszcyk", V4 := "Super_Bowl/SB_20/juszcyk/CSVs/juszcyk_20200202_193651.csv"]
all_files[Company=="poptarts"]
new_check_list <- c("sylvester_stallone","audi","facebook","ellen","pop-tarts","kobe","juszcyk")

# List all files
commercial_data <- all_files[, .N, keyby=.(keyword=Company, File=V4)]

# Add on to list
Current_Files <- commercial_data[, .N, keyby=.(keyword,File)]
# Current_Files <- Current_Files[keyword != "adp"]
# Current_Files_More <- data.table(keyword=c("lebron james","million dollar mile","alicia keys","bud light","adt","chevron","tmobile"))
# Current_Files <- rbindlist(list(Current_Files, Current_Files_More), fill=TRUE)
# setorder(Current_Files, keyword)
# Current_Files[, keyword := tolower(keyword)]
# Current_Files[keyword=="weathertech", File := "Super_Bowl/weathertech/CSVs/weathertech_20190202_185151.csv"]
# Current_Files[keyword=="bud light", File := "Super_Bowl/bud_light/CSVs/bud_light_20190203_204015.csv"]
# Current_Files[keyword=="michelob ultra", File := "Super_Bowl/michelob_ultra/CSVs/michelob_ultra_20190203_192625.csv"]
# Current_Files[keyword=="harrison ford", File := "Super_Bowl/harrison_ford/CSVs/harrison_ford_20190203_203113.csv"]
# Current_Files[keyword=="gladys knight", File := "Super_Bowl/gladys_knight/CSVs/gladys_knight_20190203_184503.csv"]
# Current_Files[keyword=="lebron james", File := "Super_Bowl/lebron_james/CSVs/lebron_james_20190203_203835.csv"]
# Current_Files[keyword=="million dollar mile", File := "Super_Bowl/million_dollar_mile/CSVs/million_dollar_mile_20190203_210054.csv"]
# Current_Files[keyword=="alicia keys", File := "Super_Bowl/alicia_keys/CSVs/alicia_keys_20190203_203931.csv"]
# Current_Files[keyword=="turkish airlines", File := "Super_Bowl/turkish_airlines/CSVs/turkish_airlines_20190203_211700.csv"]
# Current_Files[keyword=="jobs for veterns", File := "Super_Bowl/jobs_for_veterns/CSVs/jobs_for_veterns_20190203_193652.csv"]
# Current_Files[keyword=="washington post", File := "Super_Bowl/washington_post/CSVs/washington_post_20190203_212301.csv"]
# Current_Files[keyword=="simplisafe", File := "Super_Bowl/simplisafe/CSVs/simplisafe_20190203_191550.csv"]
# Current_Files[keyword=="expensify", File := "Super_Bowl/expensify/CSVs/expensify_20190203_194612.csv"]
# Current_Files[keyword=="adt", File := "Super_Bowl/adt/CSVs/adt_20190203_211920.csv"]
# Current_Files[keyword=="chevron", File := "Super_Bowl/chevron/CSVs/chevron_20190203_203721.csv"]
# Current_Files[keyword=="pizza hut", File := "Super_Bowl/Pizza_Hut/CSVs/Pizza_Hut_20190203_183147.csv"]
# Current_Files[keyword=="capillus", File := "Super_Bowl/capillus/CSVs/capillus_20190203_120744.csv"]
# Current_Files[keyword=="bumble", File := "Super_Bowl/bumble/CSVs/bumble_20190203_191402.csv"]
# Current_Files[keyword=="tmobile", File := "Super_Bowl/tmobile/CSVs/tmobile_20190203_203659.csv"]
# Current_Files[keyword=="giselle", File := "Super_Bowl/giselle/CSVs/giselle_20190203_220529.csv"]
# Current_Files[keyword=="first responders", File := "Super_Bowl/first_responders/CSVs/first_responders_20190203_220622.csv"]
# Current_Files <- unique(Current_Files)
# Current_Files[, .N, keyby=.(keyword)]
# Current_Files <- Current_Files[, .SD[1L], keyby=.(keyword)]
# Current_Files <- Current_Files[!(keyword %in% c("turbotaxlive","thejourney"))]

pull_data <- function(xPar) {
	print(xPar)
	Current_File <- Current_Files[keyword==xPar, File]
	dat <- fread(cmd=paste0("aws s3 --profile scott cp s3://havas-data-science/",Current_File," -"))
	dat[, timestamp := as.POSIXct(date, tz="America/Los_Angeles")]
	this_date <- as.Date(dat[, max(timestamp)])
	if (this_date >= as.Date("2019-02-04")) {
		dat[, timestamp := timestamp - 5*60*60]
	}
	dat[, hits := as.character(hits)]
	dat[hits=="<1", hits := "0.5"]
	dat[, hits := as.numeric(hits)]
	out_plot <- dygraph(dat[, .(timestamp, hits)], main=xPar) %>% 
		dyOptions(useDataTimezone = TRUE)	
	print(out_plot)
	dat[, File := Current_File]
	return(dat[])
}

# Loop and combine
all_dat <- lapply(Current_Files$keyword, pull_data)
all_dat <- rbindlist(all_dat, fill=TRUE)
all_dat[, keyword := as.character(keyword)]
all_dat[, keyword := tolower(keyword)]

#Save data
saveRDS(all_dat, "commercial_data_2020.rds")
