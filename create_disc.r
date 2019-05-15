library(taRifx)

all.disc <- read.csv("raw/disc.csv", sep = ",")
all.disc <- remove.factors(all.disc)

# ajusta para nomes mais apropriados (bayes, linear, kmeans)
all.disc$algo[which(all.disc$algo == "bayes")] <- "nb"
all.disc$algo[which(all.disc$algo == "kmeans")] <- "kpar"
all.disc$algo[which(all.disc$algo == "linear")] <- "linr"
all.disc$algo[which(all.disc$algo == "lr")] <- "logr"


# ajusta para nomes mais apropriados (B1, B1, G, H, L)
exp1 <- all.disc[all.disc$exp == "exp1",]
exp1$scale <- "B1"

exp2 <- all.disc[all.disc$exp == "exp2",]
exp2$scale[which(exp2$scale == "bigdata")] <- "B2"
exp2$scale[which(exp2$scale == "gigantic")] <- "G"
exp2$scale[which(exp2$scale == "huge")] <- "H"
exp2$scale[which(exp2$scale == "large")] <- "L"

exps <- rbind(exp1, exp2)
scales <- unique(exps$scale)

# parse exp1 disc
for (scale in scales){ 
  
    z <- exps[exps$scale == scale, c("algo", "hostname", "timestamp", "bytes_read", "bytes_write", 
                                    "io_read", "io_write", "time_spent_read", "time_spent_write")]
    
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
      x <- x[,c(9,1,3:8)]
      
      setwd(paste("data/", algo, sep = ""))
      write.csv(x, file = paste(algo, "_disc_", scale, ".csv", sep = ""), row.names = F, quote = F)
      
    }
}