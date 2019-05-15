setwd("~/git/new_kmeans/")
library(taRifx)

all.mem <- read.csv("raw/mem.csv", sep = ",")
all.mem <- remove.factors(all.mem)

# ajusta para nomes mais apropriados (bayes, linear, kmeans)
all.mem$algo[which(all.mem$algo == "bayes")] <- "nb"
all.mem$algo[which(all.mem$algo == "kmeans")] <- "kpar"
all.mem$algo[which(all.mem$algo == "linear")] <- "linr"
all.mem$algo[which(all.mem$algo == "lr")] <- "logr"


# ajusta para nomes mais apropriados (B1, B1, G, H, L)
exp1 <- all.mem[all.mem$exp == "exp1",]
exp1$scale <- "B1"

exp2 <- all.mem[all.mem$exp == "exp2",]
exp2$scale[which(exp2$scale == "bigdata")] <- "B2"
exp2$scale[which(exp2$scale == "gigantic")] <- "G"
exp2$scale[which(exp2$scale == "huge")] <- "H"
exp2$scale[which(exp2$scale == "large")] <- "L"

exps <- rbind(exp1, exp2)
scales <- unique(exps$scale)


# parse exp1 mem
for (scale in scales){ 
  
    z <- exps[exps$scale == scale, c("algo", "hostname", "timestamp", "buffer_cache", "free", "map", "used")]
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
      x <- x[,c(7,1,3:6)]
      
      setwd(paste("data/", algo, sep = ""))
      write.csv(x, file = paste(algo, "_mem_", scale, ".csv", sep = ""), row.names = F, quote = F)
      
    }
}