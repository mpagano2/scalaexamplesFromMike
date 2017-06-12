package odm

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row


class Tools {

  def getJOINKeyRow_KV(x : Row, key_cols_positions :Array[Int]) : Tuple2[String,Row] ={

    val JoinKeyString = key_cols_positions.map(s => x.get(s).toString).reduce(_+_)
    val row_with_join_array = new Array[Any](x.length)
    for (i <- 0 to x.length-1) {
      row_with_join_array(i) = x.get(i)
    }
    Tuple2(JoinKeyString,x)
  }
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
}
