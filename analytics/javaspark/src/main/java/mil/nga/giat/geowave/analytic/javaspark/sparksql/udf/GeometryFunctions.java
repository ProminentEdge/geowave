package mil.nga.giat.geowave.analytic.javaspark.sparksql.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.javaspark.sparksql.SimpleFeatureMapper;

public class GeometryFunctions
{
	public static void registerGeometryFunctions(
			SparkSession spark ) {
		
		spark.udf().register(
				"geomContains",
				(
						String geomString1,
						String geomString2 ) -> {
					Geometry geom1 = SimpleFeatureMapper.wktReader.read(
							geomString1);
					Geometry geom2 = SimpleFeatureMapper.wktReader.read(
							geomString2);

					return geom1.contains(
							geom2);
				},
				DataTypes.BooleanType);
		
		spark.udf().register(
				"geomIntersects",
				(
						String geomString1,
						String geomString2 ) -> {
					Geometry geom1 = SimpleFeatureMapper.wktReader.read(
							geomString1);
					Geometry geom2 = SimpleFeatureMapper.wktReader.read(
							geomString2);

					return geom1.intersects(
							geom2);
				},
				DataTypes.BooleanType);

	}
}
