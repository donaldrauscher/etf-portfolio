library(dplyr)
library(argparse)
library(yaml)

# meta data
meta <- yaml.load_file('meta.yaml')

# parse inputs
parser <- ArgumentParser()
parser$add_argument("--etf-db")
parser$add_argument("--etf-tilts")
parser$add_argument("--portfolio-summary")
parser$add_argument("--output1")
parser$add_argument("--output2")
parser$add_argument("--output3")
args <- parser$parse_args()

# pull in data
etf_db <- read.csv(args$etf_db, header = TRUE, stringsAsFactors = FALSE)
etf_tilts <- read.csv(args$etf_tilts, header = TRUE, stringsAsFactors = FALSE)
portfolio_summary <- read.csv(args$portfolio_summary, header = TRUE, stringsAsFactors = FALSE)

# format data for plot
plot1 <- etf_tilts %>%
  inner_join(etf_db, by=c("Ticker"="TICKER")) %>%
  mutate(Size = log(1+AUM/1000000000)) %>%
  mutate(Label = sprintf("Ticker: %s<br>Fund: %s<br>AUM: %1.1fB<br>E[Return]: %1.1f%%<br>SD(Return): %1.1f%%", Ticker, FUND, AUM/1000000000, Expected_Return, Expected_SD)) %>%
  rename(X=Expected_SD, Y=Expected_Return, Color=Expected_Sharpe) %>% select(X,Y,Size,Label,Color) %>% arrange(-Size)

plot1$Opacity <- seq(1, 0.3, length.out = nrow(plot1))

area_scaler <- 2/quantile(plot1$Size, 0.25)
plot1$Size <- plot1$Size * area_scaler

plot2 <- portfolio_summary %>%
  mutate(Label = sprintf("Fund: %s<br>E[Return]: %1.1f%%<br>SD(Return): %1.1f%%", Portfolio, Expected_Return, Expected_SD)) %>%
  rename(X=Expected_SD, Y=Expected_Return, Color=Expected_Sharpe) %>% select(X,Y,Label,Color) %>% arrange(X)

# create output table
plot3 <- rbind(
  etf_tilts %>% filter(Ticker %in% meta$BENCHMARKS) %>% select(Ticker, Expected_Return, Expected_Var, Expected_SD, Expected_Sharpe, Actual_Return, Actual_SD, Actual_Var, Actual_Sharpe) %>% rename(Portfolio=Ticker),
  portfolio_summary%>% select(Portfolio, Expected_Return, Expected_Var, Expected_SD, Expected_Sharpe, Actual_Return, Actual_SD, Actual_Var, Actual_Sharpe)
)

# save
write.csv(plot1, file=args$output1, row.names=FALSE)
write.csv(plot2, file=args$output2, row.names=FALSE)
write.csv(plot3, file=args$output3, row.names=FALSE)
