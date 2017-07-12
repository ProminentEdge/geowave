package mil.nga.giat.geowave.analytic.javaspark.sparksql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.geotools.data.DataUtilities;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
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

		schema = schemaFromFeatureType();
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
							feature.getAttributes());
				});
	}

	public Dataset<Row> createDataFrame() {
		return sparkSession.createDataFrame(
				rowRDD,
				schema);
	}

	private StructType schemaFromFeatureType() {
		if (featureType != null) {
			List<StructField> fields = new ArrayList<>();

			String typeSpec = DataUtilities.encodeType(
					featureType);

			LOGGER.warn(
					"Type spec: " + typeSpec);

			String[] typeList = typeSpec.split(
					",");

			for (String type : typeList) {
				String[] typePair = type.split(
						":");

				StructField field = DataTypes.createStructField(
						typePair[0],
						getDataType(
								typePair[1]),
						true);

				fields.add(
						field);
			}

			return DataTypes.createStructType(
					fields);
		}

		return null;
	}

	private static DataType getDataType(
			String javaType ) {
		// Handle geometry types first
		if (javaType.equalsIgnoreCase(
				"point")) {
			List<StructField> pointFields = new ArrayList<>();

			StructField xField = DataTypes.createStructField(
					"x",
					DataTypes.DoubleType,
					true);

			pointFields.add(
					xField);

			StructField yField = DataTypes.createStructField(
					"y",
					DataTypes.DoubleType,
					true);

			pointFields.add(
					yField);

			return DataTypes.createStructType(
					pointFields);
		}

		// Handle complex types?

		// Handle built-in types
		if (javaType.equalsIgnoreCase(
				"integer")) {
			return DataTypes.IntegerType;
		}

		if (javaType.equalsIgnoreCase(
				"double")) {
			return DataTypes.DoubleType;
		}

		if (javaType.equalsIgnoreCase(
				"date")) {
			return DataTypes.DateType;
		}

		// Default is String
		return DataTypes.StringType;
	}
}
