import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// read the data from the file in RDD api (sparkContext) and manipulate it to get the field values (in key-value pair fashion)
val rdd_Part = spark.sparkContext.textFile("/databricks-datasets/tpch/data-001/part/part.tbl")
val manipulated_Part = rdd_Part.map(rec => (rec.split("\\|")(0).toInt, (rec.split("\\|")(1),rec.split("\\|")(2), rec.split("\\|")(3),rec.split("\\|")(4))))

// read the data from the file in RDD api (sparkContext) and manipulate it to get the field values (in key-value pair fashion)
val rdd_PartSupp = spark.sparkContext.textFile("/databricks-datasets/tpch/data-001/partsupp/partsupp.tbl")
val manipulated_PartSupp = rdd_PartSupp.map(rec => (rec.split("\\|")(0).toInt, (rec.split("\\|")(1),rec.split("\\|")(2), rec.split("\\|")(3),rec.split("\\|")(4))))

// do the join using keys of 2 RDDs. Join can be done using RDD key-value pairs.
val rdd_join = manipulated_PartSupp.join(manipulated_Part)

// flatten the rdd values for the end-result 
val rdd_join_Remap = rdd_join.map(f => (f._1, 
                                        f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4,
                                        f._2._2._1, f._2._2._2, f._2._2._3, f._2._2._4))

// see the first line of the joined rdd
rdd_join_Remap.collect().take(1).foreach(println)
