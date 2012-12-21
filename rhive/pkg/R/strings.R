#' Format a vector of column names into a well formatted hive QL list
#' Inserts comma, newline and tab in between elements 
#' and appends a newline at the end
#' @param ... elements to be combined and collapsed with str_c
#' @param last whether or not the last element is the last column to be used
#' if not, appends a comma
#' @export
#' @import stringr
#' @examples
#' str_c_columns(c("a","b","c"))
str_c_columns <- function(...,last=TRUE) {
  dots_char <- lapply(list(...),as.character)
  cols <- do.call(str_c,c("\t",dots_char,collapse=",\n") )
  if (str_length(cols)>0) {
    cols <- paste(cols,ifelse(last,"\n",",\n"),sep="")
  }
  cols
}
  

