#' Aggregate rows in a hive table
#'
#' @param hivecon rhive connection
#' @param X rhive table reference
#' @param by list of column names to group by
#' @param select list of columns to aggregate
#' @param combine method of aggregation for elements in 'select' but not in 'by'
#' @param ... named list of addition methods of aggregation
#' @import stringr
#' @import rsql
#' @export
aggregate.rhivetable <- function(x,
    by,
    select,
    group=.(`*`),
    group.method='to_array',
    hivecon=rhive.options("connection"),
    name=paste0(X@name,"_group_by_",paste0(to_sql(by),collapse="_")),
    ...) {
  X = x
  X.select <- if (missing(select)) {
      list()
    }else {
      from(X,select)
    }
  X.by = from(X,by)

  if (!is.character(group.method)) {
    if (is.name(group.method)) {
      group.method = deparse(group.method)
    }else {
      stop("Unknown type for group.method:",group.method)
    }
  }
  X.by.vars = unique(sapply(X.by,all.vars))
  X.group <- sapply(from(X, group),
      function(x){
        if (all(all.vars(x) %in% X.by.vars)) {
          x
        }else {
          call(group.method,x)
        }
    })

  
  stmt <- to_sql(X$select(c(X.group,X.select)
        )$group_by(X.by)) # list(...) here because of bug
  X.aggregate <- hive.create(name,
      query=stmt,hivecon=hivecon,
      ...)
  X.aggregate
}

#' @export
setMethod("aggregate",list(x="rhivetable"),aggregate.rhivetable)
