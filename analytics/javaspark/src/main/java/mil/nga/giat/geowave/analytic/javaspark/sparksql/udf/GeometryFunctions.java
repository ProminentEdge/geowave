package mil.nga.giat.geowave.analytic.javaspark.sparksql.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.types.DataTypes;

public class GeometryFunctions
{
	public static void registerGeometryFunctions(
			SparkSession spark ) {
		UDFRegistration udf = spark.sqlContext().udf();
		
		udf.register(
				"GF_CONTAINS",
				new GFContains(),
				DataTypes.BooleanType);
//		
//		udf.registerJava(
//				"GF_CONTAINS",
//				GFContains.class.getCanonicalName(),
//				DataTypes.BooleanType);
	}
}
