This package allows Hive tables to be manipulated similar to in-memory R objects.  Currently implemented functions include: aggregate, melt, and unmelt (a limited form of dcast).

This project can be built with gradle >= 1.2

    $ gradle clean build

Alternatively, the package can be built by manually running `roxygenize` and `R CMD build .`

    $ R
    > library(roxygen2,quietly=TRUE,verbose=FALSE);roxygen2::roxygenize(package.dir='rhive/pkg',roxygen.dir='rhive/build/rhive',roclets = c("collate", "namespace", "rd", "testthat"))
    ...
    $ R CMD build rhive/build/rhive

After the package has been built, rhive can be installed either via gradle:

    $ gradle install

Or manually:

    $ R CMD INSTALL rhive/build/rhive*.tar.gz

You can use gradle to build and install the package on the Hive/Hadoop server with just

    $ gradle clean install


Note:  As with any R package, the package dependencies must be installed in order to install the package.  Of particular note for rhive are the RHadoop packages rhdfs and rmr2 which require some configuration as indicated on the [RHadoop website](https://github.com/RevolutionAnalytics/RHadoop).


In order to be able to use rhive, several variables must be set.  These are:

  * HADOOP_HOME; The root directory of the hadoop installation.
  * HADOOP_STREAMING; The location of the hadoop-streaming jar file.
  * HIVE_HOME;  The root directory of the hive installation.
  * HIVE_PORT;  The port on which the hive server is listening.  This varies with the version, but can generally be found in `$HIVE_HOME/bin/ext/hiveserver.sh`.

If these environmental variables cannot be set, they can be set within R after loading the rhive package:

    library(rhive)
    hive.init(HADOOP_HOME='/home/hadoop',...)

If these environmental variables are set, you need only call `hive.init()`.

    library(rhive)
    hive.init()

Once a connection has been established, you can create references to hive tables with `rhivetable` or create new hive tables with `to.hivetable(...)`

    htab <- to.hivetable(data.frame(x=rep(1:5,each=2),y=rnorm(10)),'rhivetab')
    tabref <- rhivetable('rhivetab')

These two references point to the same table.  These table references inherit from `rsql_table` and can be used to create rsql expressions in addition to being used in rhive methods.

    tabref$select(.(x,y > 0))$where(.(x > 5))
    aggregate(tabref,by=.(x),group.method='sum')

For more information, consult the package documentation or the documentation for the rsql package.
