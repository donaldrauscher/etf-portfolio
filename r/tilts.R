library(dplyr)
library(argparse)
library(yaml)
library(MASS)

# meta data
meta <- yaml.load_file('meta.yaml')

# parse inputs
parser <- ArgumentParser()
parser$add_argument("--returns")
parser$add_argument("--factors")
parser$add_argument("--use-aic")
parser$add_argument("--output")
args <- parser$parse_args()

# pull in data
returns <- read.csv(args$returns, header = TRUE, stringsAsFactors = FALSE)
factors <- read.csv(args$factors, header = TRUE, stringsAsFactors = FALSE)

# merge in factors
returns2 <- returns %>% inner_join(factors, by="Month")

# calculate factor tilts and residual variance
measure_tilts <- function(data){
  model <- glm(meta$REGRESSION$FORMULA, data = data)
  if (as.numeric(args$use_aic)){
    model <- stepAIC(model, trace=FALSE) 
  }
  coefficients <- as.data.frame(matrix(model$coefficients, ncol=length(model$coefficients)))
  names(coefficients) <- names(model$coefficients)
  coefficients <- coefficients %>% rename(Alpha=`(Intercept)`)
  coefficients$Residual_Variance <- var(model$residuals)
  return(coefficients)
}

tilts <- returns2 %>% group_by(Ticker) %>% do(measure_tilts(.)) %>% ungroup()

# fill in 0 if not significant
trim <- function (x) gsub("^\\s+|\\s+$", "", x)
var <- sapply(strsplit(strsplit(meta$REGRESSION$FORMULA, "~")[[1]][2], "\\+")[[1]], trim)
var2 <- c(var, "Alpha")
if (length(setdiff(var2, names(tilts)))>0){
  tilts[,setdiff(var2, names(tilts))] <- NA 
}
tilts[,var2][is.na(tilts[,var2])] <- 0

# calculate actual return and variance
stats <- returns2 %>% group_by(Ticker) %>% arrange(Month) %>% summarise(Actual_Return = 100*(prod(1+Return/100)^(12/n())-1), Actual_SD = sqrt(12)*sd(Return)) %>% 
  mutate(Actual_Var = Actual_SD^2, Actual_Sharpe = Actual_Return / Actual_SD)

# calculate expected return and variance
factor_avg <- (apply(factors[,var], 2, mean) + apply(factors[,var], 2, median))/2   # mid-point between median and avg
factor_covar <- cov(as.matrix(factors[,var]))
factor_weights <- as.matrix(tilts[,var])

tilts$Expected_Return <- as.vector(factor_weights %*% factor_avg) + tilts$Alpha
tilts$Expected_Var <- as.vector(diag(factor_weights %*% factor_covar %*% t(factor_weights))) + tilts$Residual_Variance
tilts <- tilts %>% mutate(Expected_Return = 100*((1+Expected_Return/100)^12-1), Expected_Var = 12*Expected_Var, Expected_SD = sqrt(Expected_Var), Expected_Sharpe = Expected_Return / Expected_SD)

# merge in actual stats
tilts <- tilts %>% inner_join(stats, by="Ticker")

# save
write.csv(tilts, file=args$output, row.names=FALSE)