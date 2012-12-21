#' Melt a hive table
#' First, the metadata for the columns indicated in measure.vars is gathered
#' using DESCRIBE.  This data is written, along with the index of each column
#' into a text file.  From there it is loaded into a Hive table.
#'
#' @return Returns an rhivetable containing the melted measure.vars along 
#' with id.vars.  In addition, the column names and types are returned as
#' a rhivetable reference in the 'variable.name' attribute.
#'
#' @note Future : I'd like to join the metadata with the raw melted table to
#' either add them or replace the raw indexes.
#'
#' inheritParams reshape2::melt.data.frame
#' @import reshape2
#' @export
melt.rhivetable <- function(data,id.vars,measure.vars,
    variable.name="variable",
    na.rm=!preserve.na,preserve.na=TRUE,
    value.name="value",
    name=paste0(data@name,'_melted'),
    hivecon=rhive.options("connection"),
    ...){
  X = rsql_alias(data,"data")
  X.descr <- describe(X)
  X.colnames = colnames(X)
  if (is.character(id.vars)) {
    id.vars = as.list(parse(text=id.vars))
  }
  if (is.character(measure.vars)) {
    measure.vars = as.list(parse(text=measure.vars))
  }
  if (is.character(value.name)) {
    value.name = as.quoted(value.name)
  }
  id.var.names = all.vars(id.vars)
  if (!all(id.var.names %in% X.colnames)) {
    stop("Given id.vars not found in X: ",
        id.var.names[!(id.var.names %in% X.colnames)])
  }
  measure.var.names = all.vars(measure.vars)
  if (!all(measure.var.names %in% X.colnames)) {
    stop("Given measure.vars not found in X: ",
        measure.var.names[!(measure.var.names %in% X.colnames)])
  }
  X.id = from(X,id.vars)
  X.measures = from(X,measure.vars)

  # Store column names and indexes in the measure.vars list
  # so you can join them in the query
  X.variableinfo <- X.descr[X.colnames %in% measure.vars,]
  names(X.variableinfo) <- c("name","type")
  X.variableinfo$idx <- rownames(X.variableinfo)
  metadata.table <- to.rhivetable(X.variableinfo,str_c(data@name,"_melted_variable_names"))
  metadata.alias <- rsql_alias(metadata.table,"meta")

  stmt <- to_sql(X$select(
#id variables from table
        c(from(X,id.vars),
#variable names from metadata since these are not available in the hive UDTF
          from(metadata.alias,.(name)),
#variable values from the hive_melt UDTF
          .(as(data.value__,.(value.name)))
        )$lateral_view(
#do the hive_melt, aliasing the results
          from(X,.(as(hive_melt(.(measure.vars)),var_idx__,value__)))
        )$join(metadata.alias,
#join with the metadata to get the needed variable names
          from(metadata.alias,.(idx == data.varidx__)))))

  table.melted <- hive.create(name,
        query=stmt,
        hivecon=hivecon,...)
  hive.drop(metadata.table)
  table.melted
}


#' @import reshape2
#' @export
setGeneric("melt")

#' @import reshape2
#' @export
setMethod("melt",list(data="rhivetable"),
    function(data,...)melt.rhivetable(data,...))
