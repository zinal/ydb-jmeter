# YDB plugin for Apache JMeter

YDB plugin for [Apache JMeter](https://jmeter.apache.org/) adds four test elements to JMeter:
* YDB Native Connection - configuration element to set up the connection details for YDB;
* YDB Native Request - sampler with the YDB request;
* YDB Native PostProcessor - post-processor with the YDB request;
* YDB Native PreProcessor - pre-processor with the YDB request.

The plugin supports the following types of YDB queries:
* DataQuery;
* ScanQuery;
* SchemeQuery.

For DataQuery, the transaction isolation mode can be specified. Default is SerializableRW.

For DataQuery and ScanQuery the input parameters can be specified. The parameters have to be declared in the YQL text as `$p1`, `$p2`, etc., and their data types have to be declared as `Int32`, `Text`, etc. Nullable parameters' types must have a question sign `?` at the end of type, e.g. `Int64?`.

The first row of output from DataQuery or ScanQuery can be used to fill in the output JMeter variables.

To install the plugin, its jar should be copied to the `lib/ext` subdirectory of the JMeter installation.
