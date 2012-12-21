.convert_R_type_from_hive_type <- function(types) {
    lapply(types,function(x){
        x.raw.type <- x
        expr <- quote(x)
        if (str_detect(x,"array<(.*)>")) {
          x.raw.type <- str_match(x,"array<(.*)>")[,2]
          expr <- substitute(str_split(str_match(x,"\\[(.*)\\]")[,2],",")[[1]],
            list(x=expr))
        }
        .convert_numeric <- function(x){
          as.numeric(as.character(x))
        }
        if (x.raw.type %in% c("double","tinyint","int","bigint")) {
          expr <- substitute(as.numeric(x),
            list(x=expr))
        }else {
          expr <- switch(x.raw.type,
            string=substitute(as.character(x),list(x=expr)),
            boolean=substitute(as.logical(x),list(x=expr)))
        }
        function(x){
          eval(expr,list(x=x))
        }
  })
}

#' Convert a data.frame from a string representation of a hive table to R types
#' @param x data.frame of strings from hive
#' @param types vector of hive types as strings
#' @export
convert_results <- function(x,types,x.names=names(x)) {
  convertFun <- .convert_R_type_from_hive_type(types)
  x.list <- apply(x,1,function(row){
      x.row <- data.frame(
        lapply(seq_len(length(row)),
          function(idx) convertFun[[idx]](row[[idx]])
        ))
      names(x.row) <- x.names
      list(x.row)
      })
  lapply(x.list,`[[`,1)
}

#' Convert one row from a string representation of hive types to R types
#' @param x row vector of strings
#' @param types vector of hive types as strings
#' @export
convert_result_row <- function(x,types,x.names=names(x)) {
  convertFun <- .convert_R_type_from_hive_type(types)
  x.row <- data.frame(lapply(seq_len(length(x)),
        function(idx) convertFun[[idx]](x[[idx]])
        ))
  names(x.row) <- x.names
}

#' Load an rhivetable into R
#' @param X rhivetable reference
#' @param N number of rows to select
#' @param hivecon rhivecon RJDBC connection
#' @export
#' @import stringr RJSONIO
from.hivetable <- function(X,N=NULL,hivecon=rhive.options("connection")){
  stmt <- X$select()
  stmt <- paste0(paste0(to_sql(stmt),collapse="\n"),
      if (!is.null(N)) {
        str_c("\nLIMIT ",N)
        })
  X.raw <- getQuery(hivecon,stmt)
 
  X.names = colnames(X)
  X.types = coltypes(X)
  names(X.raw) <- X.names

  convert_results(X.raw, X.types)
}

#' Push a data.frame to an rhivetable
#' @param X a data.frame to write to hive
#' @param name name to use for the generated hive table
#' @param hivecon a jdbc connection to the hive server
#' @export
#' 
to.hivetable <- function(X,name,hivecon=rhive.options("connection")) {
  hive.create(name,data=X,hivecon=hivecon)
}

#' iter method for rhivetable objects
#' A 'SELECT * FROM' statement is constructed and 
#' fetch is used to return individual rows.
#'
#' @param x the rhivetable to return an iterator for
#' @param hivecon a hive jdbc connection
#' @export
#' @S3method iter rhivetable
#' @import iterators
#' @import RJDBC
#' @import stringr
iter.rhivetable <- function(x,hivecon=rhive.options("connection"),...) {
  X.descr <- describe(x,hivecon=hivecon)
  res <- doQuery(hivecon,to_sql(x$select()))
  nextEl <- function(){
    res.row <- fetch(res,n=1)
    if (nrow(res.row)==1) {
      res.row
      names(res.row) <- X.descr[,1]
      convert_result_row(res.row,X.descr[,2])
    }else if (nrow(res.row)==0) {
      stop('StopIteration')
    }else {
      stop('Invalid number of rows returned by fetch: ',nrow(res.row))
    }
  }
  obj <- list(nextElem=nextEl)
  class(obj) <- c('irhivetable', 'abstractiter', 'iter')
  obj
}
