library(dplyr)
library(argparse)

# parse inputs
parser <- ArgumentParser()
parser$add_argument("--etf-returns")
parser$add_argument("--portfolios")
parser$add_argument("--returns-output")
parser$add_argument("--summary-output")
args <- parser$parse_args()

# pull in data
returns <- read.csv(args$etf_returns, header = TRUE, stringsAsFactors = FALSE)
portfolios <- read.csv(args$portfolios, header = TRUE, stringsAsFactors = FALSE)

# aggregate returns for each portfolio-month
returns2 <- portfolios %>% 
  left_join(returns, by=c("Ticker")) %>% 
  group_by(Portfolio, Month) %>%
  summarise(Return = sum(Weight * Return)) %>%
  rename(Ticker = Portfolio)

# create summary
portfolios2 <- portfolios %>% 
  group_by(Portfolio, Portfolio_Expected_Return, Portfolio_Expected_Var, Portfolio_Expected_SD, Portfolio_Expected_Sharpe) %>% summarise(n = n()) %>%
  rename(Expected_Return=Portfolio_Expected_Return, Expected_Var=Portfolio_Expected_Var, Expected_SD=Portfolio_Expected_SD, Expected_Sharpe=Portfolio_Expected_Sharpe)

portfolios3 <- returns2 %>% 
  rename(Portfolio = Ticker) %>%
  group_by(Portfolio) %>% arrange(Month) %>% 
  mutate(Cumulative_Return = cumprod(1+Return/100)) %>% mutate(Draw_Down = 100*(1-Cumulative_Return/cummax(Cumulative_Return))) %>%
  summarise(Actual_Return = 100*(prod(1+Return/100)^(12/n())-1), Actual_SD = sqrt(12)*sd(Return), Max_Draw_Down = max(Draw_Down)) %>% 
  mutate(Actual_Var = Actual_SD^2, Actual_Sharpe = Actual_Return / Actual_SD)

summary <- portfolios2 %>% inner_join(portfolios3, by = c("Portfolio"))
  
# save
write.csv(returns2, file=args$returns_output, row.names=FALSE)
write.csv(summary, file=args$summary_output, row.names=FALSE)
