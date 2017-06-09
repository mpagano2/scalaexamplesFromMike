package odm

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Created by docker on 6/7/17.
 */
object TestObj2 {
  def getTimestamp_(x:String) : Timestamp = {
    val format = new SimpleDateFormat("MM-dd-yyyy' 'HH:mm:ss.FFF")
    if (x.isEmpty)
      null
    else
      new Timestamp(format.parse(x).getTime)

  }

  def get_subRow(x : Row,begin_position : Int,  length : Int) : Row={
    val columnArray = new Array[Any](length+1)
    for (i <- 0 to length) {
      columnArray(i)=x.get(begin_position+ i)
    }
    Row.fromSeq(columnArray)
  }


  def Raw_compaction(s : Row,  // row in Row dataType
                     left_length : Int, // number of columns of left table
                     right_length : Int, // number of columns of right table
                     left_cdc_ts_index :Int, //index of cdc_ts_from left table
                     right_cdc_ts_index : Int  //
                      ) : Row =
  {


    if (!s.get(0).toString.trim.isEmpty && s.get(left_length).toString.trim.isEmpty)
      get_subRow(s, 0, left_length)
    else if (s.get(0).toString.trim.isEmpty && !s.get(left_length).toString.trim.isEmpty)
      get_subRow(s, left_length, right_length -1)
    else {
      if (getTimestamp_(s.get(left_cdc_ts_index).toString.trim).before(getTimestamp_(s.get(right_cdc_ts_index).toString.trim)))
        get_subRow(s, left_length, right_length -1)
      else get_subRow(s, 0, left_length)
    }
  }

  def main(args: Array[String]) {

    val gv = new GlobalContext("TestObj2")
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
    //val join_schema = StructType(df1_renamed_fields.union(df2_renamed_fields))

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
