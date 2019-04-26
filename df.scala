import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
object ProjectDF {
    def df_age_commuter(spark: SparkSession) = {
		val rac_df = spark.read.format("csv").option("header", true).load("pa_rac_S000_JT00_2015.csv")
		val od_df = spark.read.format("csv").option("header", true).load("pa_od_main_JT00_2015.csv")
		val rac_renamed = rac_df.select(expr("CA01").as("young"), expr("CA02").as("middle"), expr("CA03").as("old"))
		val od_renamed = od_df.select(expr("h_geocode"), expr("w_geocode"), expr("SA01").as("young"), expr("SA02").as("middle"), expr("SA03").as("old"))
		//sum these up
		val total_rac = rac_renamed.agg(sum("young").as("sum_young"), sum("middle").as("sum_middle"), sum("old").as("sum_old")).first.toSeq.toArray.map(_.asInstanceOf[Double])
		//remove non-commuters
		val od_filtered = od_renamed.selectExpr("substring(h_geocode, 1, 11) as home", "substring(w_geocode, 1,11) as work", "young", "middle", "old").where("home != work")
		val total_od = od_filtered.agg(sum("young").as("sum_young"), sum("middle").as("sum_middle"), sum("old").as("sum_old")).first.toSeq.toArray.map(_.asInstanceOf[Double])
		Array(total_od(0)/total_rac(0), total_od(1)/total_rac(1), total_od(2)/total_rac(2))
    }

	def df_salary_commuter(spark: SparkSession) = {
		val rac_df = spark.read.format("csv").option("header", true).load("pa_rac_S000_JT00_2015.csv")
		val od_df = spark.read.format("csv").option("header", true).load("pa_od_main_JT00_2015.csv")
		val rac_renamed = rac_df.select(expr("CE01").as("poor"), expr("CE02").as("middle"), expr("CE03").as("rich"))
		val od_renamed = od_df.select(expr("h_geocode"), expr("w_geocode"), expr("SE01").as("poor"), expr("SE02").as("middle"), expr("SE03").as("rich"))
		//sum these up
		val total_rac = rac_renamed.agg(sum("poor").as("sum_poor"), sum("middle").as("sum_middle"), sum("rich").as("sum_rich")).first.toSeq.toArray.map(_.asInstanceOf[Double])
		//remove non-commuters
		val od_filtered = od_renamed.selectExpr("substring(h_geocode, 1, 11) as home", "substring(w_geocode, 1,11) as work", "poor", "middle", "rich").where("home != work")
		val total_od = od_filtered.agg(sum("poor").as("sum_poor"), sum("middle").as("sum_middle"), sum("rich").as("sum_rich")).first.toSeq.toArray.map(_.asInstanceOf[Double])
		Array(total_od(0)/total_rac(0), total_od(1)/total_rac(1), total_od(2)/total_rac(2))
		
	}
}