library(dplyr)
library(quadprog)
library(argparse)
library(yaml)

# meta data
meta <- yaml.load_file('meta.yaml')

# parse inputs
parser <- ArgumentParser()
parser$add_argument("--tilts")
parser$add_argument("--covar")
parser$add_argument("--target-return", type="integer")
parser$add_argument("--output")
args <- parser$parse_args()

# pull in data
tilts <- read.csv(args$tilts, header = TRUE, stringsAsFactors = FALSE)
covar <- read.csv(args$covar, header = TRUE, stringsAsFactors = FALSE)

# optimize
optimize <- function(x, min_weight=0, max_weight=1, constraints=list()){
  var <- setdiff(names(meta$FACTORS_NAMES), "Alpha")
  n_etf <- length(x)
  which_etf <- tilts$Ticker %in% x
  Dmat <- 2*as.matrix(covar)[which_etf, which_etf]
  dvec <- rep(0, n_etf)
  Amat <- cbind(
    matrix(1, nrow = n_etf),
    diag(n_etf), -diag(n_etf),
    matrix(tilts$Expected_Return[which_etf], nrow = n_etf),
    as.matrix(tilts[which_etf, names(constraints)])
  )
  bvec <- c(
    1, 
    rep(c(min_weight,-max_weight), each = n_etf), 
    args$target_return, 
    as.numeric(constraints) * as.numeric(args$target_return)/100
  )
  soln <- solve.QP(Dmat, dvec, Amat, bvec, meq=1)
  return(soln)
}

# flag for outliers
is_outlier <- apply(abs(tilts[,names(meta$FACTORS_NAMES)]), 1, function(x) as.integer(sum(ifelse(x >= meta$OPTIMIZATION$COEFF_OUTLIER_CUTOFF, 1, 0))>0))

# first optimization
optimization1 <- optimize(x=tilts$Ticker[is_outlier == 0], 0, meta$OPTIMIZATION$MAX_WEIGHT, meta$OPTIMIZATION$FACTOR_TILT_MIN)
tilts$Weight <- 0
tilts$Weight[is_outlier == 0] <- optimization1$solution

# select variables for second optimization
if (meta$OPTIMIZATION$USE_CLUSTERING){
  tilts2 <- tilts %>% group_by(Cluster) %>% filter(sum(Weight) >= meta$OPTIMIZATION$MIN_WEIGHT) %>% arrange(-Expected_Sharpe) %>% slice(1) %>% ungroup()  
} else {
  tilts2 <- tilts %>% filter(Weight >= meta$OPTIMIZATION$MIN_WEIGHT)
}

# second optimization
optimization2 <- optimize(x=tilts2$Ticker, meta$OPTIMIZATION$MIN_WEIGHT, meta$OPTIMIZATION$MAX_WEIGHT, meta$OPTIMIZATION$FACTOR_TILT_MIN)
tilts3 <- tilts[tilts$Ticker %in% tilts2$Ticker, ]
tilts3$Weight <- optimization2$solution

# pull in expected return and sd/var
tilts4 <- tilts3 %>% 
  mutate(
    Portfolio = paste0("TR",ifelse(args$target_return<10,"0", ""),args$target_return), 
    Portfolio_Expected_Return = sum(Weight * Expected_Return), 
    Portfolio_Expected_Var = optimization2$value, 
    Portfolio_Expected_SD = sqrt(Portfolio_Expected_Var),
    Portfolio_Expected_Sharpe = Portfolio_Expected_Return / Portfolio_Expected_SD
  )

# save
write.csv(tilts4, file=args$output, row.names=FALSE)
