package odm

import org.apache.spark.sql.types.{StructField, StructType}

//import org.apache.log4j.{Level, Logger}

//case class temp(val1:String,val2:String,val3:String,val4:String)
//case class Person(name: String, age: Long)

object TestObj1 {

 class localContext extends GlobalContext("ff")
  {



 }

  def main(args: Array[String]) {

    val gv = new GlobalContext("TestObj1")
    println("Spark context created:" + gv.SC)
    val sqlContext = gv.sqlContext

    println ("Spark version : " + gv.SC.version)
    import sqlContext.implicits._

    val df1 =gv.left_df
    val df2 = gv.right_df

    val df1_schema = df1.schema
    val df2_schema = df2.schema

    val df1_num_columns = df1.schema.fieldNames.length
    val df2_num_columns = df2.schema.fieldNames.length

    val df1_cdc_ts_position = df1.schema.fieldIndex("cdc_ts")
    val df2_cdc_ts_position = df2.schema.fieldIndex("cdc_ts")

    println("ts positions:   " + df1_cdc_ts_position.toString +"    " + df2_cdc_ts_position.toString)

    val df2_renamed_fields = df2.schema.fields.map(s => StructField("df2_"+ s.name,s.dataType,s.nullable))
    val df1_renamed_fields = df1.schema.fields.map(s => StructField("df1_"+ s.name,s.dataType,s.nullable))
    val df1_new_schema = StructType(df1_renamed_fields)
    val df2_new_schema = StructType(df2_renamed_fields)
    val join_schema = StructType(df1_renamed_fields.union(df2_renamed_fields))

    val join_df1 = sqlContext.createDataFrame(df1.rdd,df1_new_schema)
    val join_df2 = sqlContext.createDataFrame(df2.rdd,df2_new_schema)
    join_df1.printSchema()
    join_df2.printSchema()

    val df_1_partitioned = join_df1.repartition(gv.NumPartitions,$"df1_hash_key")
    val df_2_partitioned = join_df2.repartition(gv.NumPartitions,$"df2_hash_key").cache()


    println("df1_num_columns : " + df1_num_columns.toString )
    println("df2_cdc_ts_position : " + df2_cdc_ts_position.toString )
    println("df1_cdc_ts_position : " + df1_cdc_ts_position.toString )
    println("(df1_num_columns+df2_cdc_ts_position) : " + (df1_num_columns+df2_cdc_ts_position).toString )
    println("string "  )

    val full_inc_fuj_df = df_1_partitioned.join(df_2_partitioned,df_1_partitioned("df1_hash_key") === df_2_partitioned("df2_hash_key"),"full_outer")
      .rdd

    //     .map(s =>
    //         Raw_compaction(s,df1_num_columns,
    //         df2_cdc_ts_position,df1_cdc_ts_position,df1_num_columns+df2_cdc_ts_position))


    full_inc_fuj_df.saveAsTextFile(gv.result_path + gv.result_folder )
    gv.right_df.rdd.saveAsTextFile(gv.result_path + "intersect"+gv.result_path )

  }
}



//object App {









//  def main(args: Array[String])
//  {
//    val conf = new SparkConf().setAppName("Spark_Hive_Test")
//
//    val spark = new SparkContext(conf)
//    val hiveContext = new HiveContext(spark)
//
//    //Pull from source
//    val srcPull = hiveContext.sql("select * from default.kpr_dectest")
//
//    srcPull.registerTempTable("srcPull")
//
//    //Write to target
//    hiveContext.sql("insert into default.kpr_dec_tgttest select * from srcPull")
//
//
//
//
//
//

//
//
//
//    // full_inc_fuj_df.explain(true)
//
//
//
////      .map(s => if (!s.getAs("df1_hash_keyf").toString.isEmpty && s.getAs("df2_hash_keyi").toString.isEmpty)
////                        get_subRow(s, 0, df1_num_columns)
////                      else if (s.getAs("df1_hash_keyf").toString.isEmpty && !s.getAs("df2_hash_keyi").toString.isEmpty)
////                        get_subRow(s, df1_num_columns-1, df2_num_columns)
////                      else {
////
////                        if (s.getAs[Timestamp]("df1_cdc_ts").before(s.getAs[Timestamp]("df2_cdc_ts")))
////                        get_subRow(s, df1_num_columns - 1, df1_num_columns)
////                        else if (s.getAs[Timestamp]("df1_cdc_ts").equals(s.getAs[Timestamp]("df2_cdc_ts")))
////                        get_subRow(s, df1_num_columns - 1, df1_num_columns)
////                        else get_subRow(s, 0, df1_num_columns)
////
////                      }
////    )
//
//    //val full_inc_fuj_final = sqlContext.createDataFrame(full_inc_fuj_rdd,df1_schema)
//
//
//
////    .rdd
////    .map( s =>
////      if (!s.get(1).toString.isEmpty && s.get(df1_num_columns + 1).toString.isEmpty)
////        get_subRow(s, 0, df1_num_columns)
////      else if (s.get(1).toString.isEmpty && !s.get(df1_num_columns + 1).toString.isEmpty)
////        get_subRow(s, df1_num_columns-1, df2_num_columns)
////      else {
////
////        if (s.getTimestamp(4).before(s.getTimestamp(70)))
////        get_subRow(s, df1_num_columns - 1, df1_num_columns)
////        else if (s.getTimestamp(4).equals(s.getTimestamp(70)))
////        get_subRow(s, df1_num_columns - 1, df1_num_columns)
////        else get_subRow(s, 0, df1_num_columns)
////
////      }
////  )
//
//
//
//
//    // /
////
//
////
//    // val full_inc_fuj_renamed = sqlContext.createDataFrame(full_inc_fuj_,prq_field_schema)
//
//
//
//
//
//
//
////
////
//
// //  full_inc_fuj_final.write.format("parquet").save("maprfs:///datalake/uhclake/prd/developer/idemidov/results/prq" + folderName)
//    //  full_inc_fuj_df.printSchema()
//
//
////    var prq_field_schema_prq :StructType = new StructType(df1_renamed_fields)
////
////
//    //    val full_inc_fuj = df1.join(df2,df1("partn_nbr") === df2("partn_nbr")
////                                        && df1("cnsm_id") === df2("cnsm_id")
////                                        && df1("src_cd") === df2("src_cd")
////                                        && df1("lgcy_src_id") === df2("lgcy_src_id"),"full_outer")
//
//
//
//
//
//  }
//}
