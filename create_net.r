library(taRifx)

all.net <- read.csv("raw/net.csv", sep = ",")
all.net <- remove.factors(all.net)

# ajusta para nomes mais apropriados (bayes, linear, kmeans)
all.net$algo[which(all.net$algo == "bayes")] <- "nb"
all.net$algo[which(all.net$algo == "kmeans")] <- "kpar"
all.net$algo[which(all.net$algo == "linear")] <- "linr"
all.net$algo[which(all.net$algo == "lr")] <- "logr"


# ajusta para nomes mais apropriados (B1, B1, G, H, L)
exp1 <- all.net[all.net$exp == "exp1",]
exp1$scale <- "B1"

exp2 <- all.net[all.net$exp == "exp2",]
exp2$scale[which(exp2$scale == "bigdata")] <- "B2"
exp2$scale[which(exp2$scale == "gigantic")] <- "G"
exp2$scale[which(exp2$scale == "huge")] <- "H"
exp2$scale[which(exp2$scale == "large")] <- "L"

exps <- rbind(exp1, exp2)
scales <- unique(exps$scale)


# parse exp1 net
for (scale in scales){ 
  
    z <- exps[exps$scale == scale, c("algo", "hostname", "timestamp", "recv_packets", "send_bytes", "send_packets")]
    
    algos <- unique(z$algo)
  
    for (algo in algos) {
      
      x <- z[z$algo == algo, 2:length(z)] # remove a coluna "algo"
      x$moment <- round((round(x$timestamp - x$timestamp[1],0) / 5 ) + 1,0)
      
      for (i in 1:7) {
        x$hostname[which(x$hostname == paste("cluster-cin-w-", (i-1), ".c.spark-186801.internal", sep=""))] <- i
      }
      
      # muda o nome da coluna hostname para node
      names(x)[1] <- "node"
      
      # reordena as colunas e remove a coluna timestamp
      x <- x[,c(6,1,3:5)]
      
      setwd(paste("data/", algo, sep = ""))
      write.csv(x, file = paste(algo, "_net_", scale, ".csv", sep = ""), row.names = F, quote = F)
      
    }
}