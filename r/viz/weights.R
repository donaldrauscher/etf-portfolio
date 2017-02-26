library(dplyr)
library(tidyr)
library(argparse)

# parse inputs
parser <- ArgumentParser()
parser$add_argument("--etf-db")
parser$add_argument("--portfolios")
parser$add_argument("--output")
args <- parser$parse_args()

# pull in data
etf_db <- read.csv(args$etf_db, header = TRUE, stringsAsFactors = FALSE)
portfolios <- read.csv(args$portfolios, header = TRUE, stringsAsFactors = FALSE)

# de-normalize
portfolios2 <- portfolios %>% select(Portfolio, Ticker, Weight) %>% group_by(Ticker) %>% spread(Portfolio, Weight, fill=0) %>% ungroup()
portfolios3 <- portfolios2 %>% mutate(Total = rowSums(.[2:length(.)])) %>% arrange(-Total) %>% select(-Total)

# pull in names
portfolios4 <- portfolios3 %>% inner_join(etf_db %>% select(TICKER, FUND), by=c("Ticker"="TICKER")) %>% mutate(Ticker = sprintf("%s (%s)", FUND, Ticker)) %>% select(-FUND) 

# save
write.csv(portfolios4, file=args$output, row.names=FALSE)
