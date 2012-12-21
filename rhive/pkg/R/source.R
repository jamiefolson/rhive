#' @export
#' @import rhdfs
hive.source <- function(filename,...,
    hivecon=rhive.options("connection")) {
  if (!file.exists(filename)) {
    tmpfilename = tempfile(pattern=basename(filename))
    hdfs.get(filename,tmpfilename,...)
    filename = tmpfilename
  }
  for (line in unlist(str_split(str_c(readLines(filename),collapse="\n"),";"))) {
    doQuery(hivecon,line)
  }
}
