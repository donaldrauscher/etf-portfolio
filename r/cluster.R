library(dplyr)
library(argparse)
library(yaml)

# meta data
meta <- yaml.load_file('meta.yaml')

# parse inputs
parser <- ArgumentParser()
parser$add_argument("--tilts")
parser$add_argument("--output")
args <- parser$parse_args()

# pull in data
tilts <- read.csv(args$tilts, header = TRUE, stringsAsFactors = FALSE)

# variables to cluster on
trim <- function (x) gsub("^\\s+|\\s+$", "", x)
var <- sapply(strsplit(strsplit(meta$REGRESSION$FORMULA, "~")[[1]][2], "\\+")[[1]], trim)
var <- c(var, "Alpha")

# normalize and cluster (excluding outliers)
cluster_data <- scale(as.matrix(tilts[,var]))
is_outlier <- !apply(cluster_data, 1, function(x) all(abs(x) < meta$CLUSTERING$OUTLIER_SD))
cluster_model <- kmeans(cluster_data[!is_outlier,], meta$CLUSTERING$N_CLUSTER)

closest_cluster <- function(x) {
  return(which.min(apply(cluster_model$centers, 1, function(y) sqrt(sum((x-y)^2))))[1])
}

tilts$Cluster <- NA
tilts$Cluster[!is_outlier] <- cluster_model$cluster
tilts$Cluster[is_outlier] <- apply(cluster_data[is_outlier,], 1, closest_cluster)

# save
write.csv(tilts, file=args$output, row.names=FALSE)