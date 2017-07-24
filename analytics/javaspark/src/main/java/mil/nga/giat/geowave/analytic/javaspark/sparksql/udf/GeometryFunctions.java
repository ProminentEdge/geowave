package mil.nga.giat.geowave.analytic.javaspark.sparksql.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import mil.nga.giat.geowave.analytic.javaspark.sparksql.SimpleFeatureMapper;

public class GeometryFunctions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			GeometryFunctions.class);

	public static void registerGeometryFunctions(
			SparkSession spark ) {

		spark.udf().register(
				"geomEquals",
				(
						String geomString1,
						String geomString2 ) -> {
					try {
						Geometry geom1 = SimpleFeatureMapper.wktReader.read(
								geomString1);
						Geometry geom2 = SimpleFeatureMapper.wktReader.read(
								geomString2);

						return geom1.equals(
								geom2);
					}
					catch (ParseException ex) {
						LOGGER.error(ex.getMessage());
						return false;
					}
				},
				DataTypes.BooleanType);

		spark.udf().register(
				"geomContains",
				(
						String geomString1,
						String geomString2 ) -> {
					try {
						Geometry geom1 = SimpleFeatureMapper.wktReader.read(
								geomString1);
						Geometry geom2 = SimpleFeatureMapper.wktReader.read(
								geomString2);

						return geom1.contains(
								geom2);
					}
					catch (ParseException ex) {
						LOGGER.error(ex.getMessage());
						return false;
					}
				},
				DataTypes.BooleanType);

		spark.udf().register(
				"geomWithin",
				(
						String geomString1,
						String geomString2 ) -> {
					try {
						Geometry geom1 = SimpleFeatureMapper.wktReader.read(
								geomString1);
						Geometry geom2 = SimpleFeatureMapper.wktReader.read(
								geomString2);

						return geom1.within(
								geom2);
					}
					catch (ParseException ex) {
						LOGGER.error(ex.getMessage());
						return false;
					}
				},
				DataTypes.BooleanType);

		spark.udf().register(
				"geomIntersects",
				(
						String geomString1,
						String geomString2 ) -> {
					try {
						Geometry geom1 = SimpleFeatureMapper.wktReader.read(
								geomString1);
						Geometry geom2 = SimpleFeatureMapper.wktReader.read(
								geomString2);

						return geom1.intersects(
								geom2);
					}
					catch (ParseException ex) {
						LOGGER.error(ex.getMessage());
						return false;
					}
				},
				DataTypes.BooleanType);

		spark.udf().register(
				"geomCrosses",
				(
						String geomString1,
						String geomString2 ) -> {
					try {
						Geometry geom1 = SimpleFeatureMapper.wktReader.read(
								geomString1);
						Geometry geom2 = SimpleFeatureMapper.wktReader.read(
								geomString2);

						return geom1.crosses(
								geom2);
					}
					catch (ParseException ex) {
						LOGGER.error(ex.getMessage());
						return false;
					}
				},
				DataTypes.BooleanType);

		spark.udf().register(
				"geomTouches",
				(
						String geomString1,
						String geomString2 ) -> {
					try {
						Geometry geom1 = SimpleFeatureMapper.wktReader.read(
								geomString1);
						Geometry geom2 = SimpleFeatureMapper.wktReader.read(
								geomString2);

						return geom1.touches(
								geom2);
					}
					catch (ParseException ex) {
						LOGGER.error(ex.getMessage());
						return false;
					}
				},
				DataTypes.BooleanType);

		spark.udf().register(
				"geomCovers",
				(
						String geomString1,
						String geomString2 ) -> {
					try {
						Geometry geom1 = SimpleFeatureMapper.wktReader.read(
								geomString1);
						Geometry geom2 = SimpleFeatureMapper.wktReader.read(
								geomString2);

						return geom1.covers(
								geom2);
					}
					catch (ParseException ex) {
						LOGGER.error(ex.getMessage());
						return false;
					}
				},
				DataTypes.BooleanType);

		spark.udf().register(
				"geomDisjoint",
				(
						String geomString1,
						String geomString2 ) -> {
					try {
						Geometry geom1 = SimpleFeatureMapper.wktReader.read(
								geomString1);
						Geometry geom2 = SimpleFeatureMapper.wktReader.read(
								geomString2);

						return geom1.disjoint(
								geom2);
					}
					catch (ParseException ex) {
						LOGGER.error(ex.getMessage());
						return false;
					}
				},
				DataTypes.BooleanType);

		spark.udf().register(
				"geomOverlaps",
				(
						String geomString1,
						String geomString2 ) -> {
					try {
						Geometry geom1 = SimpleFeatureMapper.wktReader.read(
								geomString1);
						Geometry geom2 = SimpleFeatureMapper.wktReader.read(
								geomString2);

						return geom1.overlaps(
								geom2);
					}
					catch (ParseException ex) {
						LOGGER.error(ex.getMessage());
						return false;
					}
				},
				DataTypes.BooleanType);
	}
}
