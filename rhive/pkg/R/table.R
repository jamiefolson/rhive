require(rsql)
#' @import rsql
#' @export
setClass(
  Class="rhivetable",
  representation=representation(
    coltypes="list",
    description="data.frame"),
  contains="rsql_table"
)

#' @export
setMethod("coltypes","rhivetable",
  function(data){
    data@coltypes
  }
)

#' Create a table reference
#' @param name table name
#' @export
#' @import stringr RJDBC
rhivetable <- function(name,type="data",hivecon=rhive.options("connection")) {
  if (!hive.exists(name,hivecon=hivecon)) {
    stop("Error: Table does not exist: ",name)
  }
  x.descr <- hive.describe(name,hivecon=hivecon)
  coltypes = as.list(x.descr[,2])
  names(coltypes) <- x.descr[,1]

  new(Class="rhivetable",
      name=name,
      description=x.descr,
      colrefs=rsql_colrefs(x.descr[,1],alias=name),
      coltypes=coltypes
     )
}

#' @export
#' @rdname describe-methods
hive.describe <- function(X,extended=FALSE,
    hivecon=rhive.options("connection")){
    if (is.null(hivecon)) {
      stop(str_c("NULL rhive connection"))
    }
    description = getQuery(hivecon,paste0("DESCRIBE ",ifelse(extended,"EXTENDED ",""),to_sql(X)))
    row.names(description) <- description[,1]
    description
}


#' Describe a hive table
#' @param object the hive table to be described
#' @export
#' @docType methods
#' @rdname describe-methods
describe <- function(X,...) {
  UseMethod("describe")
}

#' @param hivecon an 'rhive' jdbc connection to a hive server
#' @param object an 'rhive' reference to a hive table
#' @param extended whether to get basic or extended table description
#' @export
#' @rdname describe-methods
#' @import stringr RJDBC
describe.rhivetable <- function(X,extended=FALSE,hivecon=rhive.options("connection")){
  if (!extended && !is.null(X@description)) {
    X@description
  }else {
    hive.describe(X,extended=extended,hivecon=hivecon)
  }
}

#' @export
#' @rdname describe-methods
#' @import stringr RJDBC
setMethod("describe","rhivetable",describe.rhivetable)

describe.rsql_alias <- function(X,...){
  describe(X@reference)
}

#' @export
#' @rdname describe-methods
#' @import stringr RJDBC
setMethod("describe","rsql_alias",describe.rsql_alias)


#' @export
#' @rdname describe-methods
#' @import stringr RJDBC
describe.default <- function(X,extended=FALSE,hivecon=rhive.options("connection")){
  hive.describe(X,extended=extended,hivecon=hivecon)
}

#' Check if a table with the given name exists
#' @param hivecon an 'rhive' jdbc connection to a hive server
#' @param name a hive tablename to check for existence
#' @return true or false
#' @export
#' @import stringr RJDBC
hive.exists <- function(x,hivecon=rhive.options("connection")) {
  if (nrow(getQuery(hivecon,str_c("SHOW TABLES '",to_sql(x),"'")))==0) {
    FALSE
  }else {
    TRUE
  }
}

#' Test whether or not a column name is numeric
#' 
#' Checks whether a character vector of Hive column types
#' can be considered numeric.
#'
#' If X.type is not specificied, the types for all columns
#' of X are used (in order).
#'
#' @param hivecon an 'rhive' jdbc connection to a hive server
#' @param X an 'rhive' reference to a hive table
#' @param types raw type string to check if it's numeric
#' @return vector of true/false indicating whether each hive type is numeric
#' @export
#' @import stringr RJDBC
hive.is_numeric <- function(X,types=coltypes(X),hivecon=rhive.options("connection")) {
  types %in% c("tinyint","smallint","int","bigint","double")
}


