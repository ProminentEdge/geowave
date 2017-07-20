package mil.nga.giat.geowave.test.javaspark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.util.Stopwatch;

import mil.nga.giat.geowave.analytic.javaspark.GeoWaveRDD;
import mil.nga.giat.geowave.analytic.javaspark.sparksql.SimpleFeatureDataFrame;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveJavaSparkSQLIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			GeoWaveJavaSparkSQLIT.class);

	private static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";
	private static final String TEST_POLYGON_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Filter.shp";
	private static final String HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-polygon-filter.shp";
	private static final int HAIL_COUNT = 13742;
	private static final int TORNADO_COUNT = 1196;

	@GeoWaveTestStore(value = {
		// GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private static Stopwatch stopwatch = new Stopwatch();

	@BeforeClass
	public static void reportTestStart() {
		stopwatch.reset();
		stopwatch.start();
		LOGGER.warn(
				"-----------------------------------------");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"*  RUNNING GeoWaveJavaSparkSQLIT        *");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		stopwatch.stop();
		LOGGER.warn(
				"-----------------------------------------");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"* FINISHED GeoWaveJavaSparkSQLIT        *");
		LOGGER.warn(
				"*         " + stopwatch.getTimeString() + " elapsed.             *");
		LOGGER.warn(
				"*                                       *");
		LOGGER.warn(
				"-----------------------------------------");
	}

	@Test
	public void testCreateDataFrame() {
		// Set up Spark
		SparkSession spark = SparkSession
				.builder()
				.master(
						"local[*]")
				.appName(
						"JavaSparkSqlIT")
				.getOrCreate();

		JavaSparkContext context = new JavaSparkContext(
				spark.sparkContext());

		// ingest test points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		try {
			// Load RDD using spatial query (bbox)
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = GeoWaveRDD.rddForSimpleFeatures(
					context.sc(),
					dataStore);

			long count = javaRdd.count();
			LOGGER.warn(
					"DataStore loaded into RDD with " + count + " features.");

			// Create a DataFrame from the RDD
			SimpleFeatureDataFrame sfDataFrame = new SimpleFeatureDataFrame(
					spark,
					dataStore,
					null);

			LOGGER.warn(
					sfDataFrame.getSchema().json());

			Dataset<Row> df = sfDataFrame.getDataFrame(
					javaRdd);
			df.show(
					10);

			df.createOrReplaceTempView(
					"features");

			String bbox = "POLYGON ((-94 34, -93 34, -93 35, -94 35, -94 34))";

			Dataset<Row> results = spark.sql(
					"SELECT * FROM features WHERE geomContains('" + bbox + "', geom)");
			long containsCount = results.count();
			LOGGER.warn(
					"Got " + containsCount + " for geomContains test");

			results = spark.sql(
					"SELECT * FROM features WHERE geomWithin(geom, '" + bbox + "')");
			long withinCount = results.count();
			LOGGER.warn(
					"Got " + withinCount + " for geomWithin test");
			
			Assert.assertTrue(
					"Within and Contains counts should be equal",
					containsCount == withinCount);

			// Test other spatial UDFs
			String line1 = "LINESTRING(0 0, 10 10)";
			String line2 = "LINESTRING(0 10, 10 0)";
			Row result = spark.sql(
					"SELECT geomIntersects('" + line1 + "', '" + line2 + "')").head();
			
			boolean intersect = result.getBoolean(
					0);
			LOGGER.warn(
					"geomIntersects returned " + intersect);

			Assert.assertTrue(
					"Lines should intersect",
					intersect);
			
			result = spark.sql(
					"SELECT geomDisjoint('" + line1 + "', '" + line2 + "')").head();
			
			boolean disjoint = result.getBoolean(
					0);
			LOGGER.warn(
					"geomDisjoint returned " + disjoint);

			Assert.assertFalse(
					"Lines should not be disjoint",
					disjoint);

		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(
					dataStore);
			spark.close();
			context.close();
			Assert.fail(
					"Error occurred while testing a bounding box query of spatial index: '" + e.getLocalizedMessage()
							+ "'");
		}

		// Clean up
		TestUtils.deleteAll(
				dataStore);

		spark.close();
		context.close();
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
