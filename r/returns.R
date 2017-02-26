library(dplyr)
library(argparse)

# parse inputs
parser <- ArgumentParser()
parser$add_argument("--prices")
parser$add_argument("--output")
args <- parser$parse_args()

# pull in data
prices <- read.csv(args$prices, header = TRUE, stringsAsFactors = FALSE)

# format prices
prices$Date <- as.Date(prices$Date)
prices1 <- prices %>% group_by(Ticker) %>% arrange(Ticker, desc(Date)) %>% filter(row_number() != n()) %>% ungroup() %>% select(Ticker,Date,Adj.Close) %>% rename(P_end = Adj.Close)
prices2 <- prices %>% group_by(Ticker) %>% arrange(Ticker, desc(Date)) %>% filter(row_number() != 1) %>% ungroup() %>% select(Adj.Close) %>% rename(P_start = Adj.Close)
prices3 <- cbind(prices1, prices2) %>% mutate(Return = (P_end / P_start - 1)*100, Month = as.numeric(format(Date,"%Y%m")))

# save
write.csv(prices3, file=args$output, row.names=FALSE)