package odm

import org.apache.spark.sql.Row

/**
 * Created by docker on 5/18/17.
 */
class Tools {

  def getJOINKeyRow_KV(x : Row, key_cols_positions :Array[Int]) : Tuple2[String,Row] ={

    val JoinKeyString = key_cols_positions.map(s => x.get(s).toString).reduce(_+_)
    val row_with_join_array = new Array[Any](x.length)
    for (i <- 0 to x.length-1) {
      row_with_join_array(i) = x.get(i)
    }
    Tuple2(JoinKeyString,x)
  }

}
