library(dplyr)
library(argparse)
library(yaml)

# meta data
meta <- yaml.load_file('meta.yaml')

# parse inputs
parser <- ArgumentParser()
parser$add_argument("--etf-tilts")
parser$add_argument("--portfolio-tilts")
parser$add_argument("--output")
args <- parser$parse_args()

# pull in data
etf_tilts <- read.csv(args$etf_tilts, header = TRUE, stringsAsFactors = FALSE)
portfolio_tilts <- read.csv(args$portfolio_tilts, header = TRUE, stringsAsFactors = FALSE)

# filter to benchmarks and combine
etf_tilts <- etf_tilts %>% filter(Ticker %in% meta$BENCHMARKS)
tilts <- rbind(etf_tilts, portfolio_tilts)

# rename variables
tilts2 <- tilts[,c("Ticker", names(meta$FACTORS_NAMES))]
names(tilts2) <- c("Ticker", meta$FACTORS_NAMES)

# save
write.csv(tilts2, file=args$output, row.names=FALSE)
