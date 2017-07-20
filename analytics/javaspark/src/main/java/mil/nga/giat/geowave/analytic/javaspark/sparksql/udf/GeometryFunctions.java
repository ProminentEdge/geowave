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
				"geomEquals",
				(
						String geomString1,
						String geomString2 ) -> {
					Geometry geom1 = SimpleFeatureMapper.wktReader.read(
							geomString1);
					Geometry geom2 = SimpleFeatureMapper.wktReader.read(
							geomString2);

					return geom1.equals(
							geom2);
				},
				DataTypes.BooleanType);
		
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
				"geomWithin",
				(
						String geomString1,
						String geomString2 ) -> {
					Geometry geom1 = SimpleFeatureMapper.wktReader.read(
							geomString1);
					Geometry geom2 = SimpleFeatureMapper.wktReader.read(
							geomString2);

					return geom1.within(
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
		
		spark.udf().register(
				"geomCrosses",
				(
						String geomString1,
						String geomString2 ) -> {
					Geometry geom1 = SimpleFeatureMapper.wktReader.read(
							geomString1);
					Geometry geom2 = SimpleFeatureMapper.wktReader.read(
							geomString2);

					return geom1.crosses(
							geom2);
				},
				DataTypes.BooleanType);
		
		spark.udf().register(
				"geomTouches",
				(
						String geomString1,
						String geomString2 ) -> {
					Geometry geom1 = SimpleFeatureMapper.wktReader.read(
							geomString1);
					Geometry geom2 = SimpleFeatureMapper.wktReader.read(
							geomString2);

					return geom1.touches(
							geom2);
				},
				DataTypes.BooleanType);
		
		spark.udf().register(
				"geomCovers",
				(
						String geomString1,
						String geomString2 ) -> {
					Geometry geom1 = SimpleFeatureMapper.wktReader.read(
							geomString1);
					Geometry geom2 = SimpleFeatureMapper.wktReader.read(
							geomString2);

					return geom1.covers(
							geom2);
				},
				DataTypes.BooleanType);
		
		spark.udf().register(
				"geomDisjoint",
				(
						String geomString1,
						String geomString2 ) -> {
					Geometry geom1 = SimpleFeatureMapper.wktReader.read(
							geomString1);
					Geometry geom2 = SimpleFeatureMapper.wktReader.read(
							geomString2);

					return geom1.disjoint(
							geom2);
				},
				DataTypes.BooleanType);
		
		spark.udf().register(
				"geomOverlaps",
				(
						String geomString1,
						String geomString2 ) -> {
					Geometry geom1 = SimpleFeatureMapper.wktReader.read(
							geomString1);
					Geometry geom2 = SimpleFeatureMapper.wktReader.read(
							geomString2);

					return geom1.overlaps(
							geom2);
				},
				DataTypes.BooleanType);
	}
}
