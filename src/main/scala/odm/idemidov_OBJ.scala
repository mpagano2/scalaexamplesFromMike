package odm

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Created by docker on 6/12/17.
 */
object idemidov_OBJ {
  class localContext(appName: String) extends GlobalContext
  {

    SConfig.set("spark.driver.maxResultSize","0")
      .setAppName(appName)

    val sc= new SparkContext(SConfig)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

    val left_df =sqlContext.sql("select * from parquet.`maprfs:////datalake/uhclake/prd/developer/idemidov/testdata/L_cnsm_srch_full_pqt_f`" )
    val right_df = sqlContext.sql("select * from parquet.`maprfs:////datalake/uhclake/prd/developer/idemidov/testdata/L_cnsm_srch_inc_prq`")
    val left_prefix = "df1"
    val right_prefix = "df2"

  }


  def main(args: Array[String]) {

    val localconfig = new localContext("TestObj1")
    val tools =new Tools
    println("Spark context created:" + localconfig.sc)

    println ("Spark version : " + localconfig.sc.version)
    import localconfig.sqlContext.implicits._

    val df1 =localconfig.left_df
    val df2 = localconfig.right_df


    val df1_num_columns = df1.schema.fieldNames.length
    val df2_num_columns = df2.schema.fieldNames.length

    val df1_cdc_ts_position = df1.schema.fieldIndex("cdc_ts")
    val df2_cdc_ts_position = df2.schema.fieldIndex("cdc_ts")

    println("ts positions:   " + df1_cdc_ts_position.toString +"    " + df2_cdc_ts_position.toString)

    val df2_renamed_fields = df2.schema.fields.map(s => StructField("df2_"+ s.name,s.dataType,s.nullable))
    val df1_renamed_fields = df1.schema.fields.map(s => StructField("df1_"+ s.name,s.dataType,s.nullable))
    val df1_new_schema = StructType(df1_renamed_fields)
    val df2_new_schema = StructType(df2_renamed_fields)

    val join_df1 = localconfig.sqlContext.createDataFrame(df1.rdd,df1_new_schema)
    val join_df2 = localconfig.sqlContext.createDataFrame(df2.rdd,df2_new_schema)
    join_df1.printSchema()
    join_df2.printSchema()

    val df_1_partitioned = join_df1.repartition(localconfig.NumPartitions,$"df1_hash_key")
    val df_2_partitioned = join_df2.repartition(localconfig.NumPartitions,$"df2_hash_key").cache()


    println("df1_num_columns : " + df1_num_columns.toString )
    println("df2_cdc_ts_position : " + df2_cdc_ts_position.toString )
    println("df1_cdc_ts_position : " + df1_cdc_ts_position.toString )
    println("(df1_num_columns+df2_cdc_ts_position) : " + (df1_num_columns+df2_cdc_ts_position).toString )
    println("string "  )

    val full_inc_fuj_df = df_1_partitioned.join(df_2_partitioned,df_1_partitioned("df1_hash_key") === df_2_partitioned("df2_hash_key"),"full_outer")
      .rdd

    //         .map(s =>
    //             tools.Raw_compaction(s,df1_num_columns,
    //             df2_cdc_ts_position,df1_cdc_ts_position,df1_num_columns+df2_cdc_ts_position))


    full_inc_fuj_df.saveAsTextFile(localconfig.result_path + localconfig.result_folder )
    localconfig.right_df.rdd.saveAsTextFile(localconfig.result_path + "intersect"+localconfig.result_path )

  }

}
