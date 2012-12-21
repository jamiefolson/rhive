#' @import logging
logging.sql <- function(record) {
  str_c(record$timestamp," ",record$levelname,":",record$logger,"\n",
      record$msg)
}

##' @import logging
#LOG <- getLogger('com.sonamine.rhive')
#LOG$addHandler(writeToFile,file="rhive.log",level='INFO',
#    formatter=logging.sql)
#
