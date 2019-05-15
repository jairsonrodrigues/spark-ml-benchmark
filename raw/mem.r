setwd("~/git/new_kmeans")

library(cluster)    
library(caret) 
library(ClusterR) 
library(fpc)

distortion.fk.threshold = 0.85

exps <- list (c("exp1", "bigdata"), c("exp2", "bigdata"), c("exp2", "gigantic"), c("exp2", "huge"), c("exp2", "large"))
#exps <- list (c("exp1", "bigdata"), c("exp2", "huge"), c("exp2", "large"))
#exps <- list (c("exp1", "bigdata"))

algos = list ("als", "bayes", "gbt", "kmeans", "lda", "linear", "lr", "pca", "rf", "svd", "svm")
#algos = list ("lda")
for (a in algos) {      
for (tmp in exps) {

  exp = tmp[1]
  scale = tmp[2]
  
  # exp = "exp1"
  # scale = "bigdata"
  
  ind = "mem"
  folder <- paste("~/git/new_kmeans/", sep="")
  #arquivo = paste(folder, "all_", exp, "_", scale, ".csv", sep="")
  arquivo = paste(folder, "mem.csv", sep="")
  
  raw <- read.csv(arquivo)
  raw <- raw[raw$exp == exp,]
  raw <- raw[raw$scale == scale,]
  
  arquivo = "~/git/new_kmeans/"
  g_df <- paste(arquivo, "-df", ".pdf", sep="")
  g_asw <- paste(arquivo, "-asw", ".pdf", sep="") 

  

          #  === FASE 1: PRE PROCESSAMENTO ====
          mem <- raw[raw$algo == a,]
          # Ponto P = { buffer, map, used }, 4 dimensões
          # obs: t$total = buffer_cache + t$free + t$used
          #      map: número de operações de mapeamento na memória
          # irq => 2,5% das observações
          mem <- mem[,c("buffer_cache", "map", "free", "used", "total")]
          
          # transforma as dimensões free, used, buffer em percentuais
          mem$free <- round(mem$free/mem$total, 2) * 100
          mem$used <- round(mem$used/mem$total, 2) * 100 
          mem$buffer_cache <- round(mem$buffer_cache/mem$total, 2) * 100
          
          # remove a variável total, pois se tornou desnecessária após conversão
          mem$total <- NULL
          mem$map <- NULL
          mem$free <- NULL
          
          # preserva o dado original
          mem.origin <- mem
          #mem <- scale(mem)
          
          # remove métricas com menos de 25% > 0
          for (n in colnames(mem)) {
            tx = sum(mem[,n] > 0)/nrow(mem)
            #cat (n , " -> ", tx, "\n") 
            if (tx < 0.25) 
              mem <- as.data.frame(mem[, which(!names(mem) %in% c(n))])
          }
          
          # corr_df <- mem[,2:ncol(mem)]
          # highCor <- findCorrelation(cor(corr_df), cutoff=0.90)
          # mem.data.origin <- corr_df[-highCor]
          #mem.data.origin <- mem[-1]
          
          # ==== FASE DOIS (Estimativa do melhor K) ====
          # distortion_fK
          #pdf(g_df)
          # criteria <- c("distortion_fK", "variance_explained", "WCSSE", "dissimilarity", "silhouette", "AIC", "BIC", "Adjusted_Rsquared")
          opt = Optimal_Clusters_KMeans(mem, 
                                        max_clusters = 10,
                                        #num_init = 10,
                                        plot_clusters = T,
                                        verbose = F,
                                        criterion = 'distortion_fK', 
                                        fK_threshold = distortion.fk.threshold,
                                        initializer = 'kmeans++', 
                                        tol_optimal_init = 0.2)
          #dev.off()
          
          # computa best K (fK) automaticamente
          #best_k = min(which(opt < distortion.fk.threshold))
          best_k = which.min(opt)
          
          # ==== FASE 3: SEGMENTACAO ====
          fit.km <- kmeans(mem, algorithm = "Hartigan-Wong", centers = best_k, nstart=50, iter.max = 100)
          
          
          # ==== FASE 2: AVALIAÇÃO ====
          # ASW
          d=dist(mem, method = "euclidean", diag = FALSE, upper = FALSE)  
          dE2   <- d^2
          asw   <- silhouette(fit.km$cluster[rownames(mem)], dE2)
          pdf(g_asw)
          plot(asw, main = "AVG Silhouette Width - mem", xlab = "silhouette width (si)",
               ylab  = "data points", col = "gray80")

          dev.off()
          print(paste("algo: ", a, "scale: ", scale, "f(K): ", opt[best_k], "k: ", best_k, "asw: ", summary(asw)$avg.width, sep = ", "))
          #print(paste(a, scale, opt[best_k], best_k, summary(asw)$avg.width, sep = ", "))
          
          centroids <- as.data.frame(fit.km$centers)
          centroids <- centroids[with(centroids, order(used + buffer_cache)), ]
          
          #print(round(centroids[,c("buffer_cache", "map", "used")],0))
  
          #plotcluster(mem.data.origin, fit.km$cluster, main = paste(a, scale, sep = "-"))
          
          
          
          tmp <- raw[raw$algo == a,]
          mem$hostname <- sapply(tmp$hostname, function(x) { paste("worker", as.integer(substr(x, 15, 15)) + 1, sep = " ") })
          mem$moment <- round((tmp$timestamp - min(tmp$timestamp)) + 1, 0) *5
          mem$cluster <- fit.km$cluster
          
          #tmp$cluster <- fit.km$cluster
          cores <- c("green", "blue", "red", "red3","red4","yellow3","orange","black","brown","gray")
          shapes <- c(0,1,15,16,17, 9, 8)
          
          #tmp$hostname <- sapply(tmp$hostname, function(x) { paste("worker", as.integer(substr(x, 15, 15)) + 1, sep = " ") })
          #tmp$moment <- round((tmp$timestamp - min(tmp$timestamp)) + 1, 0) *5
          
          
          #for(i in 1:7) {
              # modelo 1
                # g <- ggplot(tmp, aes(y = user + system + iowait + softirq, 
                #                      x = moment, color = factor(hostname), 
                #                      shape = factor(cluster))) + theme_minimal()
                # 
                # 
                # node.data <- tmp[tmp$hostname == paste('worker ', i, sep =""),]
                # node.data.inverse <- tmp[tmp$hostname != paste('worker', i, sep =""),]
                # 
                # # plot one per node
                # 
                # g <- g + geom_point(data = node.data , size = 4) 
                # 
                # g <- g + geom_line(data = node.data, size = 0.5) 
                # 
                # g <- g + geom_point(data = node.data.inverse, size = 1) 
                # 
                # g <- g + scale_shape_manual(values = shapes) +
                #     guides(color=guide_legend(title="partition"), 
                #           shape=guide_legend(title="node:")) 
                # plot(g)
            
            
            #modelo 2
            # g <- ggplot(tmp, aes(y = used + buffer_cache, 
            #                      x = moment, color = factor(hostname), 
            #                      shape = factor(cluster))) + theme_minimal()
            # 
            # 
            # node.data <- tmp[tmp$hostname == paste('worker ', i, sep =""),]
            # node.data.inverse <- tmp[tmp$hostname != paste('worker', i, sep =""),]
            # 
            # # plot one per node
            # 
            # g <- g + geom_point(data = node.data , size = 3) 
            # 
            # #g <- g + geom_line(data = node.data, size = 0.5) 
            # 
            # g <- g + geom_point(data = node.data.inverse, size = 1, alpha = 0.5) 
            # 
            # g <- g + scale_shape_manual(values = c(15,16,17)) +
            #   guides(color=guide_legend(title="partition"), 
            #          shape=guide_legend(title="node:")) 
            # plot(g)
          #}
          
          
          # plot all nodes
          g <- ggplot(mem, aes(y = used + buffer_cache, 
                               x = moment, color = factor(cluster), 
                               shape = factor(hostname))) + theme_minimal(base_size = 22)
          
          g <- g + geom_point(size = 6.5) 
          
          # g <- g + annotate("rect", xmin=0, xmax=125, ymin=0, ymax=100, fill="green", alpha=0.1) +
          #          annotate("rect", xmin=126, xmax=280, ymin=0, ymax=100, fill="red", alpha=0.1) +
          #          annotate("rect", xmin=281, xmax=1000, ymin=0, ymax=100, fill="blue2", alpha=0.1) +
          #          annotate("rect", xmin=1001, xmax=1080, ymin=0, ymax=100, fill="red", alpha=0.1)
          
          g <- g + scale_shape_manual(values = shapes) +
                 scale_color_manual(labels = c(1:10),
                                    values = cores, 
                                    breaks=c(1:10)) +
                   guides(color=guide_legend(title="load"), 
                   shape=guide_legend(title="node")) +
                   xlab("time (seconds)") +
                   ylab("mem load (used + buffer cache)")
         # plot(g)
          
          
          # plot all nodes 2
          # g <- ggplot(tmp, aes(y = user + system + iowait + softirq, 
          #                      x = moment, color = factor(hostname), 
          #                      shape = factor(cluster))) + theme_minimal()
          # 
          # 
          # node.data <- tmp[tmp$hostname == paste('worker ', i, sep =""),]
          # node.data.inverse <- tmp[tmp$hostname != paste('worker', i, sep =""),]
          # 
          # # plot one per node
          # 
          # g <- g + geom_point(data = node.data , size = 4) 
          # 
          # #g <- g + geom_line(data = node.data, size = 0.5) 
          # 
          # g <- g + geom_point(data = node.data.inverse, size = 1, alpha = 0.25) 
          # 
          # g <- g + scale_shape_manual(values = c(15,16,17)) +
          #   guides(color=guide_legend(title="partition"), 
          #          shape=guide_legend(title="node:")) 
          # plot(g)
          
          # for (i in 0:6) {
          #     tmp2 <- tmp[tmp$hostname == paste('cluster-cin-w-', i, '.c.spark-186801.internal', sep = ""),]
          #     #plot(tmp$user, col = cores[tmp$cluster], type = "p", main = paste(scale, a, "node", i, sep = " "))
          # }
          
          
  }
}