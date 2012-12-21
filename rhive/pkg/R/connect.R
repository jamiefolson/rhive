.rhiveEnv <- new.env()

#' Set or retrieve properties of the rhive.optionsiguration
#' @param x name of a property to retrieve
#' @param ... named list of properties to assign
#' @export
rhive.options <- function(x,...){
  args <- list(...)
  if (length(args)>0){
    for (varname in names(args)) {
      assign(varname,args[[varname]],envir=.rhiveEnv)
    }
  }else  if(missing(x)){
    as.list(.rhiveEnv)
  } else { 
    .rhiveEnv[[x]]
  }
}
#
#`rhive.options<-` <- function(x,value) {
#  .rhiveEnv[[x]] <- value
#  invisible(value)
#}

#' Initialize rhive properties from environmantal variables
#' @export
hive.init <- function(host="127.0.0.1",port="10000",
    database="default",
    debug=TRUE,execute=TRUE,
    external=FALSE,
    external.location=NULL,
    HADOOP_HOME=Sys.getenv('HADOOP_HOME'),
    HADOOP_STREAMING=Sys.getenv('HADOOP_STREAMING'),
    HIVE_HOME=Sys.getenv('HIVE_HOME'),
    HIVE_PORT=Sys.getenv('HIVE_PORT'),
    ...) {
  if (HADOOP_HOME=="") {
    warning("rhive property 'HADOOP_HOME' must be set before connecting to Hive")
  }
 
  if (HADOOP_STREAMING=="") {
    warning("rhive property 'HADOOP_STREAMING' must be set before connecting to Hive")
  }
 
  if (HIVE_HOME=="") {
    warning("rhive property 'HIVE_HOME' must be set before connecting to Hive")
  }
  
  if (HIVE_PORT=="") {
    warning("rhive property 'HIVE_PORT' must be set before connecting to Hive")
  }
 
  if (external && missing(external.location)) {
    stop("Must specify external.location to use exteral tables")
  } 
  rhive.options(debug=debug,execute=execute,
      external=external,external.location=external.location,
      HADOOP_HOME=HADOOP_HOME,
      HADOOP_STREAMING=HADOOP_STREAMING,
      HIVE_HOME=HIVE_HOME,
      HIVE_PORT=HIVE_PORT,...)

  conn <- hive.connect(host=host,port=port)
  rhive.options(connection=conn)
}
 
#' Initialize the connection
#' @import RJDBC
hive.connect.jdbc <- function(host="127.0.0.1",port="10000") {
  require(RJDBC)
  require(stringr)
  HIVE_PORT <- if (missing(port)) {
    rhive.options("HIVE_PORT")
  }else {
    port
  }
  HIVE_HOME <- rhive.options("HIVE_HOME")
  HADOOP_HOME <- rhive.options("HADOOP_HOME")
  if (is.null(HADOOP_HOME) || HADOOP_HOME == "") {
    stop("Must define environmental variable 'HADOOP_HOME'")
  }
  if (is.null(HIVE_HOME) || HIVE_HOME == "") {
    stop("Must define environmental variable 'HIVE_HOME'")
  }
  if (is.null(HIVE_PORT) || HIVE_PORT == "") {
    stop("Must define environmental variable 'HIVE_PORT'")
  }
  
  hivedrv <- JDBC("org.apache.hadoop.hive.jdbc.HiveDriver",
    c(list.files(HADOOP_HOME,pattern="jar$",full.names=T),
      list.files(str_c(HIVE_HOME,"/lib"),pattern="jar$",full.names=T)))
  .RHIVEDRV <- hivedrv
  hivecon <- dbConnect(hivedrv,
      str_c("jdbc:hive://localhost:",HIVE_PORT,"/default")) 
 hivecon
}
#' Connect to the Hive database
#' @param database the name of the database to connect to
#' @export
hive.connect <- function(host="127.0.0.1",port,type="jdbc",database="default") {
 
  hivecon <- switch(type,
      jdbc=hive.connect.jdbc,
      default=stop("Only jdbc connections are currently supported"))(host=host,port=port)
 
  rhive.options(connection=hivecon)

  # Do this since the JDBC driver doesn't allow you to 
  # choose database
  doQuery(hivecon,paste("USE",database))
  
  hive.doQuery(paste0("ADD JAR ",rhive.options("HADOOP_STREAMING")))

  jarDir <- str_c(path.package("rhive"),"/java/")
  for (jarFile in list.files(jarDir)) {
    hive.doQuery(paste0("ADD JAR ",jarDir,jarFile),conn=hivecon)
  }
  
  scriptDir <- str_c(path.package("rhive"),"/hive/")
  for (scriptFile in list.files(scriptDir)) {
    hive.source(paste0(scriptDir,scriptFile),conn=hivecon)
  }

  invisible(hivecon)
}

#' @export
hive.doQuery <- function(stmt,conn=rhive.options("connection"),...) {
  if (is.null(conn)) {
    stop("You must first initialize a Hive connection with hive.connect")
  }
  doQuery(conn,stmt,...)
}

#' @export
hive.getQuery <- function(stmt,conn=rhive.options("connection"),...) {
  if (is.null(conn)) {
    stop("You must first initialize a Hive connection with hive.connect")
  }
  getQuery(conn,stmt,...)
}

#' @export
doQuery <- function(conn,stmt,...) {
  LOG$info(str_c("doQuery:\n",stmt))
  if (rhive.options("debug")) {
    message(stmt)
  }
  if (rhive.options("execute")) {
    UseMethod("doQuery")
  }
}

#' @export
#' @S3method doQuery JDBCConnection
doQuery.JDBCConnection <- function(conn,stmt,...) {
  dbSendQuery(conn,stmt,...)
}

#' @export
getQuery <- function(conn,stmt,...) {
  LOG$info(str_c("doQuery:\n",stmt))
  if (rhive.options("debug")) {
    message(stmt)
  }
  if (rhive.options("execute")) {
    UseMethod("getQuery")
  }
}

#' @export
#' @S3method getQuery JDBCConnection
getQuery.JDBCConnection <- function(conn,stmt,...) {
  dbGetQuery(conn,stmt)
}
