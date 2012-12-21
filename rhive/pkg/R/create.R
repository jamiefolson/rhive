
RHIVE_STORAGE_FORMAT = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileAsTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileOutputFormat'"
create_table <- function(name,col.names,col.types,
    query,format,external,external.location,hivecon,execute=TRUE) {
  define_cols <- !missing(col.names) && !missing(col.types)
  stmt = str_c("CREATE TABLE ",name,
        if (define_cols && missing(query)) {
          str_c(" (\n",
            str_c_columns(col.names," ",col.types),
            "\n )\n")
        }else {
          "\n"
        },
        format,
        if (!missing(query)) {
          str_c("\n AS ",query)
        },
        if (external) {
          str_c("\nLOCATION '",external.location,"/",name,"/'")
        }
      )
  if (execute) {
    if (external) {
      sql.file <- tempfile(str_c(name,"_sql"))
      writeLines(stmt,con=sql.file)
      system(str_c("hadoop fs -put ",sql.file," ",external.location,"/",name,".sql"))
    }


    doQuery(hivecon,stmt)
    rhivetable(name,hivecon=hivecon)
  }else {
    stmt
  }
}
check_name <- function(name,existing="stop",hivecon) {
# If a table with the name exists, do the thing indicated by the user
  existing.options <- c("stop","rename.existing","rename","replace")
  if (! existing %in% existing.options) {
    stop("Invalid value for 'existing' : '",existing,"'",
        "\nUse one of:\n\t",str_c(existing.options,collapse=", "))
  }
  if (hive.exists(name,hivecon=hivecon)) {
    if (existing=="stop") {
      stop("Table already exists with name: ",name)
    }else if (str_detect(existing,"^rename")) {
      # generate unique name
      rename = str_c(name,"_",floor(as.numeric(Sys.time())))
      if (existing == "rename.existing") {
        # rename the existing table
        hive.rename(name,rename,hivecon=hivecon)
        doQuery(hivecon,stmt)
      }else if (existing == "rename") {
        # rename the new one
        name = rename
      }else {
        stop("Unknown rename action for existing table: ",existing)
      }
    }else if (existing == "replace") {
      # Drop the old one, the new one will replace it
      warning("Dropping existing table: ",name)
      hive.drop(name)
    }else {
      stop("Unknown action for action table: ",existing)
    }
  }
  name
}
  
data_to_table <- function(data,hivecon) {
  col.names <- str_replace_all(names(data),"\\.","_")
  data <- data.frame(lapply(data,function(x) {
        if (is.factor(x)) {
          as.character(x)
        }else {
          x
        }
      }),stringsAsFactors=FALSE)
  col.types <- sapply(data,function(x) {
      if (is.numeric(x) && (is.integer(x) || (x == floor(x)))) {
        "int"
      } else if (is.real(x)) {
        "double"
      }else {
        "string"
      }
    })
  data.tempfile <- tempfile("tmp_data")
  # Use write.table since write.csv ignores col.names
  write.table(data,file=data.tempfile,sep=",",
      quote=FALSE,  # Hive does not correctly parse quotes, so leave them out
      col.names=FALSE,row.names = FALSE)

  data.tempname = basename(data.tempfile)
  create_table(name=data.tempname,col.names=col.names,col.types=col.types,
      format="ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \n\tLINES TERMINATED BY '\\n'\nSTORED AS TEXTFILE",
      external=FALSE,
      hivecon=hivecon)
  stmt = str_c("LOAD DATA LOCAL INPATH 'file://",data.tempfile,"' INTO TABLE ",data.tempname)
  doQuery(hivecon,stmt)
  rhivetable(data.tempname,hivecon)
}

#' Rename a Hive table
#' @param from original name
#' @param to name to set
#' @param hivecon hive connection
#' @export
hive.rename <- function(from,to,hivecon=rhive.options("connection")) {
    res <- doQuery(hivecon,str_c("ALTER TABLE ",as.character(from)," RENAME TO ",as.character(to)))
    invisible(res)
}

#' Drop a Hive table
#' @param X table to drop
#' @param hivecon hive connection
#' @export
hive.drop <- function(X,hivecon=rhive.options("connection")) {
  res <- doQuery(hivecon,str_c("DROP TABLE ",as.character(X)))
    invisible(res)
}

#' List Hive tables
#' @param X table to list
#' @param hivecon hive connection
#' @export
hive.list_tables <- function(X,hivecon=rhive.options("connection")) {
  res <- getQuery(hivecon,paste0("SHOW TABLES",if(!missing(X)){
          paste0("'",to_sql(x),"'")
        })) 
  res
}


#' Create table, possibly from a query
#' Uses a pre-determined format for ease of loading into R as well as Hadoop streaming
#'
#' @param hivecon an 'rhive' jdbc connection to a hive server
#' @param name a name to try to use, if it exists the time is appended
#' @param cols a list(column_name=type_spec_string) defining the columns
#' @param query a query to use to define the table columns
#' @export
#' @import stringr RJDBC rsql
hive.create <- function(name,
    cols=NULL,col.names=names(cols),col.types=unlist(cols),
    data,query,overwrite=TRUE,
    existing=c("stop","replace","rename","rename.existing"), 
    external=rhive.options("external"),
    external.location=rhive.options("external.location"),
    format,
    hivecon=rhive.options("connection")) {
  existing <- if(missing(existing)) {
    "stop"
  }else {
    existing[1]
  }
  format <- if (missing(format)) {
    RHIVE_STORAGE_FORMAT
  }else {
    format
  }
  existing = match.arg(existing,c("stop","replace","rename",
        "rename.existing"))
  # if no external.location provided, use the connection's default
  external.location <- if (missing(external.location)) { 
    rhive.options("external.location")
  }else {
    external.location
  }
  external <- if (missing(external)) { 
    rhive.options("external")
  }else {
    external
  }
  name <- check_name(name=name,existing=existing,hivecon=hivecon)
  # check that the arguments make sense to construct a query
  content_provided <- !is.null(col.names) || !missing(query) || !missing(data)
  if (external) {
    if (is.null(external.location)) {
      stop("You must specify an external location to create an external table")
    }
  }else if (!content_provided) {
    stop("You must provide one of the column structure, data to be copied,",
        " or a defining query")
  }
  #store resulting table
  X <- NULL
  defined_cols <- !is.null(cols) || !missing(col.names) || !missing(col.types)
  if (!missing(query)) {
    if (!missing(data)) {
      stop("Only one of query and data can be provided")
    }
    if (defined_cols) {
      warning("Do not specify columns when providing a query")
    }
    # Cannot create external tables using AS
    X <- create_table(name=name,query=to_sql(query),
        format=format,hivecon=hivecon,
        external=FALSE)
    if (external) {
      descr <- describe(X,hivecon=hivecon)
      name.internal <- str_c(name,"_internal")
      hive.rename(name,name.internal,hivecon=hivecon)
      X <- create_table(name=name,col.names=descr[,1],col.types=descr[,2],
          format=format,external=external,external.location=external.location,
          hivecon=hivecon)
      doQuery(hivecon,str_c("INSERT ",ifelse(overwrite,"OVERWRITE","INTO"),
            " TABLE ",name," SELECT * FROM ",name.internal))
      hive.drop(name.internal,hivecon=hivecon)
    }   
  }else if (!missing(data)){
    if (!is.data.frame(data)) {
      stop("Currently, data must be provided as a data.frame, instead of :",class(data)[1])
    }
    if (defined_cols) {
      warning("Do not specify columns when providing a data object")
    }
    data.temp <- data_to_table(data,hivecon=hivecon)
    descr <- describe(data.temp,hivecon=hivecon)
    
    X <- create_table(name=name,col.names=descr[,1],col.types=descr[,2],
        format=format,external=external,external.location=external.location,
        hivecon=hivecon)
    doQuery(hivecon,str_c("INSERT ",ifelse(overwrite,"OVERWRITE","INTO"),
        " TABLE ",name," SELECT * FROM ",to_sql(data.temp)))
    hive.drop(data.temp,hivecon=hivecon)
  }else if (!is.null(col.names)) {
    if (is.null(col.types)) {
      stop("If column information is provided, both name and type must be given")
    }else {
      X <- create_table(name=name,col.names=col.names,col.types=col.types,
          format=format,external=external,external.location=external.location,
          hivecon=hivecon)
    }
  }
  X
}


  
 
