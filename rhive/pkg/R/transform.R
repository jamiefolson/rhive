#' Hive TRANSFORM using r functions
#' @export
#' @import rmr2 rhdfs
hive.transform <- function(map, reduce=NULL, 
    input, input.cols="*",input.format=c("sequence.typedbytes","native"),
    output, output.colspec,output.format = c("sequence.typedbytes","native"),
    vectorized=TRUE,
    verbose=TRUE, debug=FALSE,
    execute=TRUE,
    hivecon=rhive.options("connection")) {
  
  if (hive.exists(output,hivecon=hivecon)) {
    if (missing(output.colspec)) {
      output.descr <- describe(output,hivecon=hivecon)
      output.colspec <- output.descr[,2]
      names(output.colspec) <- output.descr[,1]
    }
  }else {
    output.colspec <- if (missing(output.colspec)) {
      list('key'='BINARY','value'='BINARY')
    }else {
      output.colspec
    }
    output <- hive.create(output,cols=output.colspec,hivecon=hivecon)
  }
  if (!is(output,"rhivetable")) {
    output <- rhivetable(as.character(output),hivecon=hivecon)
  }
  input.descr = describe(input,hivecon=hivecon)
  if (input.cols == "*") {
    input.cols = input.descr[,1]
  }
  input.format <- match.arg(input.format,c("sequence.typedbytes","native"))
  output.format <- match.arg(output.format,c("sequence.typedbytes","native"))

 hiveTransform(map=map,
      reduce=reduce,
      input=input,input.cols=input.cols,input.format=input.format,
      output=output,output.colspec=output.colspec,output.format=output.format,
      vectorized=vectorized,
      verbose=verbose,debug=debug,execute=execute,hivecon=hivecon)
}

#' Hive TRANSFORM using r functions
#' @export
#' @import rmr2 rhdfs
hiveTransform = function(
  map, 
  reduce, 
  input,
  input.cols,
  input.format,
  output,
  output.colspec,
  output.format,
  vectorized=TRUE,
  verbose = TRUE, 
  debug = FALSE,
  execute=TRUE,
  hivecon) {
  rhive.input.format=make.input.format("sequence.typedbytes")
  rhive.output.format=make.output.format(
      mode="binary",
      format=rmr2:::make.typedbytes.output.format())

  native.input.format=make.input.format("native")
  native.output.format=make.output.format("native")
  keyval.length = 1000

  keyval.wrapper <- function(k,v,key.names=NULL,value.names=NULL) {
        rmr2:::keyval(lapply(k,
            function(x) { 
              # Do this if using typedbytes vector/list instead of array(145)
              x = lapply(x,unlist) 
              names(x) = key.names
              x
            }),
          lapply(v,
            function(x){
              # Do this if using typedbytes vector/list instead of array(145)
              x = lapply(x,unlist)
              names(x) = value.names
              x
            }))
  }

  reduce.wrap <- function(reduce,keyval.wrap=TRUE,...) {
    reduce.fun=reduce
    function(k,v) {
      kv <- if (keyval.wrap) {
        keyval.wrapper(k,v,...)
      }else {
        keyval(k,v)
      }
      reduce.fun(keys(kv),values(kv))
    }
  }

  map.wrap <- function(map,vectorized=TRUE,keyval.wrap=FALSE,...) {
    map.fun=map
    function(k,v) {
      kv <- if (keyval.wrap) {
        keyval.wrapper(k,v,...)
      }else {
        keyval(k,v)
      }
      out <- if (vectorized) {
        map.fun(keys(kv), values(kv))
      }else {
        out <- rmr2:::c.keyval(mapply(function(k,v){
                kv.one = map.fun(k,v)
                keyval(list(keys(kv.one)),list(values(kv.one)))
              },keys(kv),values(kv),SIMPLIFY=FALSE))
      }
      out
    }
  }
  

 ## prepare map and reduce executables
  rmr.local.env = tempfile(pattern = "rmr-local-env")
  rmr.global.env = tempfile(pattern = "rmr-global-env")
  preamble = paste(sep = "", '#!/usr/bin/env Rscript

  sink(stderr(),type="output")

  options(warn=1)
  profile.nodes=FALSE
 
  library(rmr2)
  library(rhdfs)
  hdfs.init()
  message("loaded RHadoop")
  status <- hdfs.get("',rmr.local.env,'",getwd())
  if (!status) {
    stop("Could not load local environment")
  }
  status <- hdfs.get("',rmr.global.env,'",getwd())
  if (!status) {
    stop("Could not load global environment")
  }
  message("got rmr env")
  load("',basename(rmr.global.env),'")
  load("',basename(rmr.local.env),'")
  message("loaded rmr env")
  invisible(lapply(libs, function(l) require(l, character.only = T)))
  
  rhive.input.reader = 
    function()
      rmr2:::make.keyval.reader(
        rhive.input.format$mode, 
        rhive.input.format$format, 
        keyval.length = keyval.length)
  rhive.output.writer = 
    function()
      rmr2:::make.keyval.writer(
        rhive.output.format$mode, 
        rhive.output.format$format)
    
  native.input.reader = 
    function() 
      rmr2:::make.keyval.reader(
        native.input.format$mode, 
        native.input.format$format, 
        keyval.length = keyval.length)
  native.output.writer = 
    function() 
      rmr2:::make.keyval.writer(
        native.output.format$mode, 
        native.output.format$format)
  
  map.FUN = map
  map = if (is.null(map.FUN)) {
    NULL
  }else {
    map.wrap(map=map.FUN,
      keyval.wrap=(input.format=="sequence.typedbytes"),
      vectorized=vectorized,
      value.names = input.cols)
  }
  reduce.FUN = reduce
  reduce = if (is.null(reduce.FUN)) {
    NULL
  }else {
    reduce.wrap(reduce=reduce.FUN,
      keyval.wrap=(is.null(map) && input.format=="sequence.typedbytes"),
      value.names = input.cols)
  }
  
  sink(type="output")
  ')  
  map.line = '  
  rmr2:::map.loop(
    map = map, 
    keyval.reader = rhive.input.reader(), 
    keyval.writer = 
      if(is.null(reduce)) {
        rhive.output.writer()}
      else {
        native.output.writer()},
    profile = profile.nodes)
  '
  reduce.line  =  '  
  rmr2:::reduce.loop(
    reduce = reduce, 
    keyval.reader = native.input.reader(), 
    keyval.writer = rhive.output.writer(),
    profile = profile.nodes)'

  map.file = tempfile(pattern = "rmr-streaming-map")
  writeLines(c(preamble, map.line), con = map.file)
  status <- system(sprintf("chmod 775 %s", map.file), ignore.stderr = TRUE)
	if(status != 0) {
		warning("no executable found")
		invisible(FALSE)
	}
  map.name <- basename(map.file)
  map.file.hdfs <- paste("hdfs:///tmp/rhive/",map.name,sep="")
  hdfs.put(map.file,map.file.hdfs)
  hive.doQuery(paste("ADD FILE",map.file.hdfs))
  
  reduce.file = tempfile(pattern = "rmr-streaming-reduce")
  writeLines(c(preamble, reduce.line), con = reduce.file)
  status <- system(sprintf("chmod 775 %s", reduce.file), ignore.stderr = TRUE)
	if(status != 0) {
		warning("no executable found")
		invisible(FALSE)
	}
  reduce.name <- basename(reduce.file)
  reduce.file.hdfs <- paste("hdfs:///tmp/rhive/",reduce.name,sep="")
  hdfs.put(reduce.file,reduce.file.hdfs)
  hive.doQuery(paste("ADD FILE",reduce.file.hdfs))
  
  save.env = function(fun = NULL, name) {
    envir = 
      if(is.null(fun)) parent.env(environment()) else {
        if (is.function(fun)) environment(fun)
        else fun}
    save(list = ls(all.names = TRUE, envir = envir), file = name, envir = envir)
    name}
  
  
  libs = sub("package:", "", grep("package", search(), value = T))
  image.cmd.line = paste("-file",
                         c(save.env(name = rmr.local.env),
                           save.env(.GlobalEnv, rmr.global.env)),
                         collapse = " ")
  hdfs.put(rmr.local.env,paste("hdfs://",rmr.local.env,sep=""))
  hdfs.put(rmr.global.env,paste("hdfs://",rmr.global.env,sep=""))

  rhive.serde="ROW FORMAT SERDE 'com.jfolson.hive.serde.RTypedBytesSerDe'"
  rhive.record.writer = "RECORDWRITER 'com.jfolson.hive.serde.RTypedBytesRecordWriter'"
  rhive.record.reader="RECORDREADER 'com.jfolson.hive.serde.RTypedBytesRecordReader'"

  map.result = "AS (key BINARY, value BINARY)"

  keylength = switch(input.format,
      sequence.typedbytes=0,
      native=1)
  map.input.serde.props = paste("'native' = '",tolower(input.format=="native"),"',",
      "'keylength' = '",keylength,"'",sep="")
  map.output.serde.props = paste("'native' = '",tolower(!is.null(reduce) || 
        (output.format=="native")),"'",sep="")

  if (is.null(reduce)) {
      map.result = paste("AS (",paste(names(output.colspec),output.colspec,collapse=", ",sep=" "),")",sep="")
      map.output.serde.props = "'native' = 'false'"
   }
   map.input.format = paste(rhive.serde,"\n\t\t",
       "WITH SERDEPROPERTIES ( ",map.input.serde.props," )\n\t",
       rhive.record.writer,sep="")
   map.output.format = paste(rhive.serde,"\n\t\t",
        "WITH SERDEPROPERTIES ( ",map.output.serde.props," ) \n\t",
        rhive.record.reader,sep="")
   reduce.input.format = paste(rhive.serde,"\n\t\t",
       "WITH SERDEPROPERTIES ( 'native' = 'true' )\n\t",
       rhive.record.writer,sep="")
   reduce.output.format = paste(rhive.serde,"\n\t\t",
        "WITH SERDEPROPERTIES ( 'native' = '",tolower(output.format=="native"),"' ) \n\t",
        rhive.record.reader,sep="")
  
  stmt=paste("FROM (\n\tFROM ",as.character(input),
      " input \n\t",
      "SELECT TRANSFORM (",
        paste("input.",input.cols,collapse=", ",sep=""),
      ")\n\t",
      map.input.format,"\n\t",
      "USING '",map.name,"'\n\t",
      map.result,"\n\t",
      map.output.format,
      "\n\t", 
      if (!is.null(reduce)) {
        "DISTRIBUTE BY key\n\t"
      },
      ") map_output\nINSERT OVERWRITE TABLE ",as.character(output),sep="")

  if (is.null(reduce)) {
    stmt <- paste(stmt,"\n\tSELECT map_output.*",sep="")
  }else {
    stmt <- paste(stmt,"\n\t",
        "SELECT TRANSFORM(map_output.key, map_output.value)\n\t",
        reduce.input.format,"\n\t",
        "USING '",reduce.name,
        "'\n\tAS (",paste(output.select,collapse=", "),")\n\t",
        reduce.output.format,sep="")
  }
  if (execute) {
    doQuery(hivecon,stmt)
    output
  }else {
    stmt
  }
}

