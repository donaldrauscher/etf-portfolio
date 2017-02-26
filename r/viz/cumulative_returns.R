library(dplyr)
library(tidyr)
library(argparse)
library(yaml)

# meta data
meta <- yaml.load_file('meta.yaml')

# parse inputs
parser <- ArgumentParser()
parser$add_argument("--etf-returns")
parser$add_argument("--portfolio-returns")
parser$add_argument("--output")
args <- parser$parse_args()

# pull in data
etf_returns <- read.csv(args$etf_returns, header = TRUE, stringsAsFactors = FALSE)
portfolio_returns <- read.csv(args$portfolio_returns, header = TRUE, stringsAsFactors = FALSE)

# filter to benchmarks
etf_returns <- etf_returns %>% filter(Ticker %in% meta$BENCHMARKS) %>% select(Ticker, Month, Return)

# cumulative returns and de-normalize
returns <- rbind(etf_returns, portfolio_returns)
returns2 <- returns %>% group_by(Ticker) %>% arrange(Month) %>% mutate(Cumulative_Return = cumprod(1+Return/100)) %>% ungroup()
returns3 <- returns2 %>% select(-Return) %>% group_by(Month) %>% spread(Ticker, Cumulative_Return) %>% ungroup()

# format month
returns3$Month <- sapply(returns3$Month, function(x) paste(substr(x, 1, 4), substr(x, 5, 7), "01", sep = "-"))

# save
write.csv(returns3, file=args$output, row.names=FALSE)
