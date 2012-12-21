
#' Filter the rows of X according to a variety of different rules
#' The arguments select and subset can be passed either as R expressions
#' to be deparsed or as character values.
#'
#' Many Hive expressions, such as logical constructs like 'x > 5'
#' are also valid R expressions, allowing them to be passed (but
#' not evaluated) to the subset function.  However, other valid Hive
#' expressions, like 'DISTINCT x' are not valid R expressions and
#' must be passed (and evaluated) instead as character values.
#'
#' Actual construction of the query in quite straightforward, with
#' the subset argument (if present) defining the WHERE clause and
#' the select argument determining the columns to be selected.
#'
#' @param X the rhivetable to filter
#' @param select a character vector of column names, or TRUE to select *
#' @param subset a logical condition in the Hive query language
#'  to use to define the filter
#'
#' @return A new rhivetable with the desired subset of the original.
#' 
#' @export
subset.rhivetable <- function(x,
    subset,
    select=.(`*`),
    name=paste0(x@name,"_subset"),
    hivecon=rhive.options("connection"),
    ...) { 
  X = x
  X.ref <- rsql_alias(X,"x")
  select <- if (missing(select)) {
      TRUE
    }else {
      select
    }
  stmt = X.ref$select(select)
  if (!missing(subset)) {
    stmt = stmt$where(from(X.ref,subset))
  }
  stmt <- to_sql(stmt)
  hive.create(name,
      query=stmt,
      hivecon=hivecon,...)
}

#' @export
setMethod("subset",list(x="rhivetable"),subset.rhivetable)

