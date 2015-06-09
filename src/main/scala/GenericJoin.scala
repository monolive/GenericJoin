/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._


object GenericJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Generic Join")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("DROP TABLE IF EXISTS dataset8")
    sqlContext.sql("CREATE TABLE dataset8 STORED AS ORC AS select dataset1_bucket.key as key, dataset1_bucket.att1, dataset1_bucket.att2, dataset1_bucket.att3, dataset1_bucket.att4, dataset1_bucket.count1, dataset1_bucket.count2, dataset1_bucket.count3, dataset1_bucket.count4, dataset1_bucket.count5, dataset1_bucket.count6, dataset1_bucket.count7, dataset1_bucket.count8, dataset1_bucket.count9, dataset1_bucket.count10, dataset1_bucket.count11, dataset1_bucket.count12, dataset1_bucket.count13, dataset1_bucket.count14, dataset1_bucket.count15, dataset1_bucket.count16, dataset1_bucket.amount1, dataset1_bucket.amount2, dataset1_bucket.amount3, dataset1_bucket.amount4, dataset1_bucket.amount5, dataset1_bucket.amount6, dataset1_bucket.amount7, dataset1_bucket.amount8, dataset1_bucket.amount9, dataset1_bucket.amount10, dataset1_bucket.amount11, dataset1_bucket.amount12, dataset1_bucket.amount13, dataset1_bucket.amount14, dataset1_bucket.amount15, dataset1_bucket.amount16, bucket_dataset2.att1 as att5, bucket_dataset2.att2  as att6, bucket_dataset2.att3 as att7, bucket_dataset2.att4 as att8, bucket_dataset2.count1 as count17, bucket_dataset2.count2 as count18, bucket_dataset2.count3 as count19, bucket_dataset2.count4 as count20, bucket_dataset2.count5 as count21, bucket_dataset2.count6 as count22, bucket_dataset2.count7 as count23, bucket_dataset2.count8 as count24, bucket_dataset2.count9 as count25, bucket_dataset2.count10 as count26, bucket_dataset2.count11 as count27, bucket_dataset2.count12 as count29, bucket_dataset2.count13 as count30, bucket_dataset2.count14 as count31, bucket_dataset2.count15 as count32, bucket_dataset2.count16 as count33, bucket_dataset2.amount1 as amount17, bucket_dataset2.amount2 as amount18, bucket_dataset2.amount3 as amount19, bucket_dataset2.amount4 as amount20, bucket_dataset2.amount5 as amount21, bucket_dataset2.amount6 as amount22, bucket_dataset2.amount7 as amount23, bucket_dataset2.amount8 as amount24, bucket_dataset2.amount9 as amount25, bucket_dataset2.amount10 as amount26, bucket_dataset2.amount11 as amount27, bucket_dataset2.amount12 as amount28, bucket_dataset2.amount13 as amount29, bucket_dataset2.amount14 as amount30, bucket_dataset2.amount15 as amount31, bucket_dataset2.amount16 as amount32 from dataset1_bucket LEFT OUTER JOIN bucket_dataset2 ON dataset1_bucket.key = bucket_dataset2.key")

  }
}
