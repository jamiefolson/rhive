LOG <- NULL

#' @import stringr 
#' @import logging
.onLoad <- function(libname, pkgname) {
  hive.init()
  rhive:::LOG <- getLogger('com.jfolson.rhive')
  rhive:::LOG$addHandler(writeToFile,file="rhive.log",level='INFO',
      formatter=logging.sql)

}
