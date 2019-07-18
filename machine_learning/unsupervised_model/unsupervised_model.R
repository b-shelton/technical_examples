#load required packages

packages <- c("data.table",
              "stats",
              "ggplot2",
              "dplyr",
              "tidyr")

new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)
for (p in packages) library(p, character.only = TRUE, quietly = TRUE)

# Create local directory where to download data
data_path <- "medicare_clustering_results"

if (file.exists(data_path)) {

  setwd(paste0("~/", data_path))

  } else {

  dir.create(file.path(data_path))
  setwd(paste0("~/", data_path))

}

# Ingest data from AWS s3 object.
ca <- read.csv(url("https://s3.us-east-2.amazonaws.com/example.data/medicare_california_pain_mgmt.csv"), header = TRUE)
ca <- data.table(ca)

#create a new field titled "lines_per_bene"
ca$lines_per_bene <- ca$line_srvc_cnt / ca$bene_unique_cnt


ca <- ca[, c("npi",
             "provider_type",
             "hcpcs_code",
             "line_srvc_cnt",
             "bene_unique_cnt",
             "bene_day_srvc_cnt",
             "lines_per_bene")]

#transform (cast wide) focal variables for evaluation by the k-means clustering algorithm
d1 <- dcast(ca, npi ~ hcpcs_code, value.var = c("line_srvc_cnt"), sum)
d2 <- dcast(ca, npi ~ hcpcs_code, value.var = c("bene_unique_cnt"), sum)
d3 <- dcast(ca, npi ~ hcpcs_code, value.var = c("bene_day_srvc_cnt"), sum)
d4 <- dcast(ca, npi ~ hcpcs_code, value.var = c("lines_per_bene"), sum)

colnames(d1) <- paste("line_cnt:", colnames(d1))
colnames(d2) <- paste("bene_unique_cnt:", colnames(d2))
colnames(d3) <- paste("bene_day_srvc_cnt:", colnames(d3))
colnames(d4) <- paste("lines_per_bene:", colnames(d4))
colnames(d1)[1] <- colnames(d2)[1] <- colnames(d3)[1] <- colnames(d4)[1] <- "npi"

final.a <- dplyr::left_join(d1, d2, by = "npi")
final.b <- dplyr::left_join(final.a, d3, by = "npi")
final <- dplyr::left_join(final.b, d4, by = "npi")

npi <- final$npi
final$npi <- NULL
final <- scale(final) #normalize the predictor variables
dim(final)

#evaluate the percentage of cluster-explained variance of different cluster counts
cluster_range <- c(2:14)
pct_var <- data.frame(pct_var = 0,
                      change = 0,
                      num_clusters = cluster_range)
totalss <- kmeans(final, centers = 14, nstart = 10)$totss
for(i in cluster_range){
  pct_var[i-1, 'pct_var'] <- kmeans(final, centers = i, nstart = 10)$betweenss/totalss
}
for(i in 2:13){
  pct_var[i, 'change'] <- pct_var[i, 'pct_var'] - pct_var[i-1, 'pct_var']
}

pct_var_chart <- ggplot(data = pct_var, aes(x = num_clusters, y = pct_var)) +
  geom_line() +
  geom_point() +
  scale_x_continuous(breaks = c(min(pct_var$num_clusters):max(pct_var$num_clusters))) +
  geom_line(data = pct_var, aes(x = num_clusters, y = change, colour = "red")) +
  theme(legend.position = "none") +
  labs(title = "Percentage of Variance Explained by Cluster Count (black line)")

print(pct_var_chart)
ggsave("variance_explained_by_cluster_count.jpg")
print(pct_var)

#run k-means analysis
k1 <- 8
km1 <- kmeans(final, centers = k1, nstart = 10)
clusters <- data.table(npi = npi, cluster = as.factor(km1$cluster))

centers <- data.frame(cluster = factor(1:k1), km1$centers)
centers <- data.frame(t(centers ))
for(i in 1:ncol(centers)) {
  centers[, i] <- as.numeric(as.character(centers[, i]))
}
colnames(centers) <- paste("Cluster", c(1:k1))
centers$Symbol <- row.names(centers)
centers <- gather(centers, "Cluster", "Mean", -Symbol)
centers$Color = centers$Mean > 0
centers <- subset(centers, Symbol != "cluster")

#function to view the details of different clusters. x = the cluster number to view (e.g., 1)
cluster_details <- function(x) {
  t <- ca[npi %in% clusters[cluster == x]$npi]
  print(paste0("Number of unique NPIs: ", nrow(t[which(!duplicated(t$npi))])))
  print(c(t[which(!duplicated(t$npi))]$npi))
  print(t)
}

#function to view the HCPCS code utilization differences between the different clusters, displaying only the HCPCS codes of the focal cluster.
# x = the cluster number to view
# y = the variable to display (e.g., "lines_per_bene")
cluster_chart <- function(x, y) {
  t <- data.table(ca)[npi %in% clusters[cluster == x]$npi]$hcpcs_code
  u <- centers[grepl(paste(t, collapse = "|"), centers$Symbol) == TRUE & grepl(y, centers$Symbol) == TRUE, ]
  u$hcpcs <- as.character(unique(unlist(regmatches(u$Symbol, gregexpr("[0-9]+", u$Symbol)))))
  v <- ggplot(u, aes(x = hcpcs, y = Mean, fill = Color)) +
    geom_bar(stat = 'identity', position = "identity", width = .75) +
    facet_grid(Cluster ~ .) +
    theme(axis.text.x = element_blank(),
          axis.text.y = element_text(size = 7),
          plot.title = element_text(size=12),
          legend.position = "none") +
    labs(y = "Standard Deviations from Mean",
         title = paste0("Standard Deviations from Means Comparison '", y,
                             "' Belonging to Cluster ", x))
  print(v)

  width = 700
  height = width / 1.75
  ggsave("cluster_variable_differences.jpg", width = width, height = height, units = "mm")
}

cluster_chart(which.max(km1$size), "lines_per_bene")

cluster_sizes <- data.frame(cluster = c(1:length(km1$size)),
                            provider_count = km1$size)
print(cluster_sizes)
