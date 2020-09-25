// Here is the solution with Dataset API (and then in the end register it as sql table for more comfortable ad-hoc query experience)

// Here is the Regex pattern to parse the log (can be debugged using one of the Java Regex testers in the internet)
val logRegex = """^(\S+) - - \[(\S+) [+]\d{4}] "(\S+) (\S+) (\S+)" (\S+) (\S+) "(\S+)" "(.*?)"$""".r; 

// lets read the log files into a dataset
val logFile = spark.read.textFile("/databricks-datasets/learning-spark/data-001/fake_logs/log1.log").as[String]

// lets create the case class for the schema
case class log(ip:String, dateTime:String, methodType:String, uri:String, http_version:String, response_code:String, content_size:String, address:String, protocol:String);

// lets create a function that parses the logs of returned values.
def parseLog(line: String):log = {
    val logRegex(ip, dateTime, methodType, uri, http_version, response_code, content_size, address, protocol) = line;
    return log(ip, dateTime, methodType, uri, http_version, response_code, content_size, address, protocol);
}

// lets parse the dataset of logs - line by line using map function.
val logs = logFile.map(line => parseLog(line));

// lets create a sql table for a better query experience
logs.createOrReplaceTempView("logs");

display(logs)
//logs.printSchema
