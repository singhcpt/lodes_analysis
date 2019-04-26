import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD 
object ProjectRDD {
    def rdd_age_commuter(sc: SparkContext) = {
        val rac_data = sc.textFile("pa_rac_S000_JT00_2015.csv").map(_.split(",")).filter(x => !(x(0).contains("geocode")))
        val od_data = sc.textFile("pa_od_main_JT00_2015.csv").map(_.split(",")).filter(x => !(x(0).contains("geocode")))
        // Mapped so that we have relvant Age data. Equivalent to map/reduce code
        val rac_mapped = rac_data.map(x => (x(0), Array(x(2).toDouble, x(3).toDouble, x(4).toDouble) ) )
        val od_mapped = od_data.map(x => (x(1), (x(0), Array(x(3).toDouble, x(4).toDouble, x(5).toDouble) ) ) )
        // Filtering out non-commuters
        val od_filtered = od_mapped.filter(x => !(x._1.substring(0,11) == x._2._1.substring(0,11) ) )

        // Reduce and sum up values in array     
		val total_rac = rac_mapped.reduce( (acc , curr) => ("sum", Array(acc._2(0) + curr._2(0),  acc._2(1) + curr._2(1), acc._2(2) + curr._2(2))))
		val total_od = od_filtered.reduce( (sum, curr) => ("sum", ("sum", Array(sum._2._2(0) + curr._2._2(0),  sum._2._2(1) + curr._2._2(1), sum._2._2(2) + curr._2._2(2)))))
		Array(total_od._2._2(0)/total_rac._2(0), total_od._2._2(1)/total_rac._2(1), total_od._2._2(2)/total_rac._2(2))
    }
	def rdd_salary_commuter(sc: SparkContext) = {
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