library(dplyr)
library(tidyr)
library(corpcor)
library(argparse)
library(yaml)

# meta data
meta <- yaml.load_file('meta.yaml')

# parse inputs
parser <- ArgumentParser()
parser$add_argument("--returns")
parser$add_argument("--tilts")
parser$add_argument("--tilts2") # not used
parser$add_argument("--factors")
parser$add_argument("--output")
args <- parser$parse_args()

# pull in data
returns <- read.csv(args$returns, header = TRUE, stringsAsFactors = FALSE)
tilts <- read.csv(args$tilts, header = TRUE, stringsAsFactors = FALSE)
factors <- read.csv(args$factors, header = TRUE, stringsAsFactors = FALSE)

# create covariances for factors
factor_names <- setdiff(names(factors), c("Month","RF"))
factor_covar <- cov.shrink(as.matrix(factors[,factor_names]))
factor_weights <- as.matrix(tilts[,factor_names])
etf_factor_covar <- factor_weights %*% factor_covar %*% t(factor_weights)

# residual variance
if (meta$REGRESSION$UNCORRELATED_RESIDUALS){
  
  etf_residual_covar <- diag(tilts$Residual_Variance)

} else {

  # pull out residuals
  add_residuals <- function(data){
    model <- glm(meta$REGRESSION$FORMULA, data = data)
    data$Residuals <- model$residuals
    return(data)
  }
  
  returns2 <- returns %>% inner_join(factors, by="Month") %>% group_by(Ticker) %>% do(add_residuals(.)) %>% ungroup()
  
  # calc covariances between residuals
  returns3 <- returns2 %>% select(Ticker, Month, Residuals) %>% group_by(Month) %>% spread(Ticker, Residuals) %>% ungroup()
  etf_residual_covar <- cov.shrink(as.matrix(na.omit(returns3 %>% select(-Month))))
  
}

# add together and annualize
etf_covar <- 12*(etf_factor_covar + etf_residual_covar)

# save
write.csv(etf_covar, file=args$output, row.names=FALSE)