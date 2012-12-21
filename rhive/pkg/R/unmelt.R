#' Unmelt an object
#' @export
unmelt <- function(data,...) {
  UseMethod("unmelt")
}

#' Unmelt a hive table
#' @inheritParams reshape2::melt.data.frame
#' @export
#' @S3method unmelt rhivetable
unmelt.rhivetable <- function(data,id.vars=list(),
    variable.name="Variable",
    value.name="Value",
    name=paste0(data@name,'_unmelted'),
    hivecon=rhive.options("connection"),
    ...) {
  X = rsql_alias(data,"dat")
  if (is.character(id.vars)) {
    id.names = id.vars
    id.vars = as.quoted(id.vars)
    names(id.vars) <- id.names
  }
  variable <- if (is.character(variable.name)) {
    as.quoted(variable.name)
  }else {
    variable.name
  }
  value <- if (is.character(value.name)) {
    as.quoted(value.name)
  }else {
    value.name
  }
  X.id = from(X,id.vars)
  X.variable = from(X,variable)
  X.value = from(X,value)
  stmt = to_sql(X$select(c(X.id,
        list(melted=.(to_map(
              if(.(X.variable) == "" || is.null(.(X.variable))) "NA" 
                else .(X.variable),
            .(X.value))
            ))))$group_by(X.id))
  X.tomap <- hive.create(str_c(X@name,"_to_map"),query=stmt,hivecon=hivecon,
      existing='rename')

  var.names <- getQuery(hivecon,
      to_sql(X$select(.(distinct(.(X.variable))))))
  var.names = var.names[,1]
  na.names = str_length(var.names)==0 | is.na(var.names)
  var.names = var.names[!na.names]
  if (any(na.names) && (! "NA" %in% var.names)) {
    var.names = c(var.names,"NA")
  }

  var.unmelt = lapply(var.names,
      function(name)substitute(as(melted[x],alias),
      list(x=name,alias=as.quoted(name))))

  stmt <- to_sql(X.tomap$select(c(from(X.tomap,id.vars),var.unmelt)))
  X.unmelted <- hive.create(name=name,
      query=stmt,hivecon=hivecon,...)
  hive.drop(X.tomap)
  X.unmelted
  }

#' @export
setMethod("unmelt",list(data="rhivetable"),unmelt.rhivetable)
