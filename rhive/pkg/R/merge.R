#' Perform the appropriate join query on two Hive tables and return the result.
#' Similar to the merge.data.frame, combines rows in x and y where by.x == by.y.
#' The important difference here is that this is performed on Hive tables
#' using the Hive query language.  Most arguments have the same meaning as in
#' merge.data.frame.  The values of all.x and all.y are used to determine the
#' type of join necessary.  In addition, select.y = FALSE can be used to 
#' specify a LEFT SEMI JOIN which selects values in by.x that appear in by.y
#' without actually including/joining any values in y.
#' 
#' Not all parameters from base:merge.data.frame are supported
#' @inheritParams base::merge.data.frame
#' @param select.x which columns from x to select or TRUE to select all
#' @param select.y which columns from y to select or TRUE to select all
#' @param .name name to use for the merged tables
#' or FALSE to select none and perform a LEFT SEMI JOIN
#' @param hivecon jdbc connection to the hive server
#' @return a rhivetable reference to the merged result
#' @export
merge.rhivetable <- function(x, y, by,
               by.x = by, by.y = by, all = FALSE, all.x = all, all.y = all,
               select.x=.(`*`), select.y=.(`*`),
               suffixes = c(str_c("_",to_sql(x)),str_c("_",to_sql(y))),
               name=paste0(x@name,'_merge_',y@name), 
               hivecon=rhive.options("connection"),
               ...) {

  x.ref <- rsql_alias(x,"x")
  y.ref <- rsql_alias(y,"y")
  suffix.x <- suffixes[1]
  suffix.y <- suffixes[2]
  x.descr <- describe(x,hivecon=hivecon)
  y.descr <- describe(y,hivecon=hivecon)
  include.x <- TRUE
  include.y <- TRUE
  select.x <- if (missing(select.x)) {
      TRUE
    }else {
      select.x
    }
  
  select.y <- if (missing(select.y)) {
      TRUE
    }else {
      select.y
    }
  
  if (!is.null(select.x) && length(select.x)>0 && select.x!=FALSE) {
    include.x <- TRUE
  }else {
    stop("Hive does not currently support RIGHT SEMI JOIN")
    include.x <- FALSE
  }
  if (!is.null(select.y) && length(select.y) > 0 && select.y!=FALSE) {
    include.y <- TRUE
  }else {
    include.y <- FALSE
  }
  if (!(include.y || include.x)) {
    stop("You must specify values in either x or y to be selected")
  }
 
  if (!include.y) {
    suffix.x <- ""
  }
 
  select.x <- if (include.x) {
    from(x.ref,select.x,suffix=suffix.x)
  }else {
    list()
  }
  select.y <- if (include.y) {
    from(y.ref,select.y,suffix=suffix.y)
  }else {
    list()
  }

  by.x = if (missing(by.x)) { by }else { by.x }
  by.x <- from(x.ref,by.x)
  by.y = if (missing(by.y)) { by }else { by.y }
  by.y <- from(y.ref,by.y)

  join <- ""
  if (all.x) {
    if (!include.y) {
      stop("Invalid arguments: all.x is TRUE,",
          " but no columns selected from y, so join does nothing")
    }
    if (all.y) {
      join <- "FULL OUTER"
    }else {
      join <- "LEFT OUTER"
    }
  }else if (all.y) {
    join <- "RIGHT OUTER"
  }else if (!include.y) {
    join <- "LEFT SEMI"
  }

  stmt <- to_sql(rsql_select(select.x,select.y)$from(
        x.ref)$join(y.ref,.(.(by.x) == .(by.y)),method=join))
  xy.join <- hive.create(name,query=stmt,hivecon=hivecon,...)
  xy.join
}

#' @export
setMethod("merge","rhivetable",merge.rhivetable)
