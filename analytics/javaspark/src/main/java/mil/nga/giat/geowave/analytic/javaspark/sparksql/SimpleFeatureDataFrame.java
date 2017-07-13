package mil.nga.giat.geowave.analytic.javaspark.sparksql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.analytic.javaspark.sparksql.udt.Feature;
import mil.nga.giat.geowave.analytic.javaspark.sparksql.udt.FeatureUDT;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class SimpleFeatureDataFrame
{
	private static Logger LOGGER = LoggerFactory.getLogger(
			SimpleFeatureDataFrame.class);

	private final SparkSession sparkSession;
	private final SimpleFeatureType featureType;
	private final StructType schema;
	private JavaRDD<Row> rowRDD;

	public SimpleFeatureDataFrame(
			final SparkSession sparkSession,
			final DataStorePluginOptions dataStore,
			final ByteArrayId adapterId ) {
		this.sparkSession = sparkSession;

		featureType = FeatureDataUtils.getFeatureType(
				dataStore,
				adapterId);

		schema = new StructType().add(
				"feature",
				new FeatureUDT(
						featureType),
				false);
	}

	public SimpleFeatureType getFeatureType() {
		return featureType;
	}

	public StructType getSchema() {
		return schema;
	}

	public JavaRDD<Row> getRowRDD() {
		return rowRDD;
	}

	public void initRowRDD(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD ) {
		rowRDD = pairRDD.values().map(
				feature -> {
					return RowFactory.create(
							new Feature(
									feature));
				});
	}

	public Dataset<Row> createDataFrame() {
		return sparkSession.createDataFrame(
				rowRDD,
				schema);
	}
}
