package mil.nga.giat.geowave.analytic.javaspark.sparksql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
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
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

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
	private StructType schema;
	private JavaRDD<Row> rowRDD;
	private static WKTWriter wktWriter = new WKTWriter();
	private static WKTReader wktReader = new WKTReader();

	public SimpleFeatureDataFrame(
			final SparkSession sparkSession,
			final DataStorePluginOptions dataStore,
			final ByteArrayId adapterId ) {
		this.sparkSession = sparkSession;

		featureType = FeatureDataUtils.getFeatureType(
				dataStore,
				adapterId);

		schema = schemaFromFeatureType(
				featureType);
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
					Serializable[] fields = new Serializable[schema.size()];

					for (int i = 0; i < schema.size(); i++) {
						StructField structField = schema.apply(
								i);
						if (structField.name().equals(
								"geom")) {
							fields[i] = wktWriter.write(
									(Geometry) feature.getAttribute(
											i));
						}
						else if (structField.dataType() == DataTypes.TimestampType) {
							fields[i] = ((Date) feature.getAttribute(
									i)).getTime();
						}
						else if (structField.dataType() != null) {
							fields[i] = (Serializable)feature.getAttribute(
									i);
						}
						else {
							fields[i] = "";
						}
					}

					return RowFactory.create(
							fields);
				});
	}

	public Dataset<Row> createDataFrame() {
		return sparkSession.createDataFrame(
				rowRDD,
				schema);
	}

	private static StructType schemaFromFeatureType(
			SimpleFeatureType featureType ) {
		List<StructField> fields = new ArrayList<>();

		for (AttributeDescriptor attrDesc : featureType.getAttributeDescriptors()) {
			SimpleFeatureDataType sfDataType = attrDescToDataType(
					attrDesc);

			String fieldName = (sfDataType.isGeom() ? "geom" : attrDesc.getName().getLocalPart());

			StructField field = DataTypes.createStructField(
					fieldName,
					sfDataType.getDataType(),
					true);

			fields.add(
					field);
		}

		return DataTypes.createStructType(
				fields);
	}

	private static SimpleFeatureDataType attrDescToDataType(
			AttributeDescriptor attrDesc ) {
		boolean isGeom = false;
		DataType dataTypeOut = DataTypes.NullType;

		if (attrDesc.getType().getBinding().equals(
				String.class)) {
			dataTypeOut = DataTypes.StringType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Double.class)) {
			dataTypeOut = DataTypes.DoubleType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Float.class)) {
			dataTypeOut = DataTypes.FloatType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Long.class)) {
			dataTypeOut = DataTypes.LongType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Integer.class)) {
			dataTypeOut = DataTypes.IntegerType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Boolean.class)) {
			dataTypeOut = DataTypes.BooleanType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Date.class)) {
			dataTypeOut = DataTypes.TimestampType;
		}

		// Custom geometry types get WKB encoding
		else if (Geometry.class.isAssignableFrom(
				attrDesc.getType().getBinding())) {
			dataTypeOut = DataTypes.StringType;
			isGeom = true;
		}

		return new SimpleFeatureDataType(
				dataTypeOut,
				isGeom);
	}
}
