import org.apache.spark.sql.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
object ProjectDF {
    def df_age_commuter(sc: SparkContext) = {
	val rac_df = spark.read.format("csv").option("header", true).load("pa_rac_S000_JT00_2015.csv")
	val od_df = spark.read.format("csv").option("header", true).load("pa_od_main_JT00_2015.csv")
	val rac_renamed = rac_df.select(expr("CA01").as("young"), expr("CA02").as("middle"), expr("CA03").as("old"))
	val od_renamed = od_df.select("h_geocode", "w_geocode", expr("SA01").as("young"), expr("SA02").as("middle"), expr("SA03").as("old"))
	//sum these up
	val sums = rac_renamed.agg(sum("young").as("sum_young"), sum("middle").as("sum_middle"), sum("old").as("sum_old")).first
	//remove non-commuters
	df.selectExpr("substring(value, 0, 2)", "substring(value, 1, 2)", "substring(value, 2,2)", "substring(value, 3,2)").show
    }



	def df_salary_commuter(sc: SparkContext) = {
		val rac_data = sc.textFile("pa_rac_S000_JT00_2015.csv").map(_.split(",")).filter(x => !(x(0).contains("geocode")))
        val od_data = sc.textFile("pa_od_main_JT00_2015.csv").map(_.split(",")).filter(x => !(x(0).contains("geocode")))
        // Mapped so that we have relevant Salary data. Equivalent to map/reduce code
        val rac_mapped = rac_data.map(x => (x(0), Array(x(5).toDouble, x(6).toDouble, x(7).toDouble) ) )
        val od_mapped = od_data.map(x => (x(1), (x(0), Array(x(6).toDouble, x(7).toDouble, x(8).toDouble) ) ) )
        // Filtering out non-commuters
        val od_filtered = od_mapped.filter(x => !(x._1.substring(0,11) == x._2._1.substring(0,11) ) )

        // Reduce and sum up values in array     
		val total_rac = rac_mapped.reduce( (acc , curr) => ("sum", Array(acc._2(0) + curr._2(0),  acc._2(1) + curr._2(1), acc._2(2) + curr._2(2))))
		val total_od = od_filtered.reduce( (sum, curr) => ("sum", ("sum", Array(sum._2._2(0) + curr._2._2(0),  sum._2._2(1) + curr._2._2(1), sum._2._2(2) + curr._2._2(2)))))
		Array(total_od._2._2(0)/total_rac._2(0), total_od._2._2(1)/total_rac._2(1), total_od._2._2(2)/total_rac._2(2))
		
	}
}