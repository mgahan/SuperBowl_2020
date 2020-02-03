
# Bring in libraries
library(data.table)
library(gtrendsR)
setwd("/home/mgahan/SuperBowl_2019")

# Key companies
key_companies <- c("Microsoft Surface","T Mobile","Amazon Prime Video","Quibi","GMC",
									 "Jeep","Walmart","Google Assistant","Reeses","Rocket Mortgage",
									 "Tide","Bounty","Charmin","Genesis","John Legend","Chrissy Tiegen",
									 "Pepsi Zero Sugar","Amazon Echo","Ellen DeGeneres","Discover Card",
									 "Hard Rock","Little Caesars","Anquan Boldin","Verizon","Turkish Airlines",
									 "Planters","Squarespace","Cheetos","mc hammer","Doritos","HoneyRacha",
									 "Budweiser","Michelob","jimmy fallon","Bud Light","Girls Who Code",
									 "SodaStream","Mike Bloomberg","Donald Trump","Coke Energy","TurboTax","Sabra",
									 "Facebook","Sylvester Stallone","Toyota","Snickers","New York Life",
									 "Avocados From Mexico","Kia","WeatherTech","Taycan","Audi","Smaht Pahk",
									 "Pop-Tarts","Pringles","Mountain Dew","Jennifer Lopez","Shakira","Kobe",
									 "Demi Lovato","Demi Lovato time","Joe Buck","Troy Aikman","Mike Pereira",
									 "Len Dawson","hank stram","the rock","portabellas","yolanda adams","top gun",
									 "secret","lego masters","roman numerals","5GBuiltRight","mulan",
									 "jake from state farm","youtubetv","heres to the next 100","fast saga","alice johnson",
									 "matriculating the ball down the field","tom brady","hulu","winona",
									 "bryan cranston","super monday","black widow","theavonetwork.com","turbotax","jonah hill",
									 "dot dot dot","bsabynut","planters","james bond","masked singer","google","how do i remember","urkel",
									 "5G","hummer","lebron james","minions","fox nation","smackdown","super bowl","work absence excuses",
								   "toby","ja rule","is that pitbull?","saint archer","subway","bill nye","george kemp","hunters",
									 "heinz","old town road","juszcyk","ellen","snail","jellyfish","beyonce","jayz","kevin hart","paul mccartney",
									 "murdoch","inspirecahnge","puppy bowl","harrahs","super bowl mvp","disney +","xfl","facebook",
									 "man napping","alexa")

key_companies <- tolower(key_companies)
key_companies <- unique(key_companies)

# Read in data
retrieve_data_func <- function(company_par) {
	
	# Current time company_par
	print(company_par)
	Current_Time <- Sys.time()
	Current_Time <- gsub("\\s+","_",gsub("[-:]","",Current_Time))
	
	# Update Company name
	Company_Name <- gsub("\\s+", "_", company_par)
	
	# Create outfiles
	out_rds <- paste0(Company_Name,"_",Current_Time,".rds")
	out_csv <- paste0(Company_Name,"_",Current_Time,".csv")
	
	# Pull data
	dat_list <- gtrends(company_par, time = "now 4-H", geo = "US") 
	
	# Organize data
	dat <- as.data.table(dat_list$interest_over_time)
	dat[, date := date-3*60*60]
	dat[, date := as.character(date)]
	fwrite(dat, out_csv)
	saveRDS(dat_list, out_rds)
	
	# Upload to S3
	upload_txt_rds <- paste0("aws s3 --profile scott mv ", out_rds, " s3://havas-data-science/Super_Bowl/SB_20/",Company_Name,"/Lists/",out_rds)
	upload_txt_csv <- paste0("aws s3 --profile scott mv ", out_csv, " s3://havas-data-science/Super_Bowl/SB_20/",Company_Name,"/CSVs/",out_csv)
	upload_sys_rds <- system(upload_txt_rds, intern=TRUE)
	upload_sys_csv <- system(upload_txt_csv, intern=TRUE)
	
	# Return output
	return(company_par)
	
}

# Error handling
retrieve_data_trycatch <- function(company_par_iter) {
	output_attempt <- tryCatch(
		{
			retrieve_data_func(company_par=company_par_iter)
		},
		error=function(cond) {
			return(paste0(company_par_iter," had error"))
		},
		warning=function(cond) {
		},
		finally={
			return(paste0(company_par_iter," had error"))
		}
	)
	return(output_attempt)
}


# Create loop
for (xComp  in key_companies) {
		retrieve_data_trycatch(company_par_iter=xComp)
		Sys.sleep(30)
}
print(Sys.time())

# Asis schedule
# */45 * * * *
# dat <- fread("aws s3 --profile scott cp s3://havas-data-science/Super_Bowl/SB_20/capillus/CSVs/capillus_20190202_162901.csv -")
# dat[, timestamp := as.POSIXct(date, format="%Y-%m-%d %H:%M:%S")]
# library(dygraphs)
# dygraph(dat[, .(timestamp, hits)])