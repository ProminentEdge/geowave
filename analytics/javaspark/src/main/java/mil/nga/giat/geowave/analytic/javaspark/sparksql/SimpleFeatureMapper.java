package mil.nga.giat.geowave.analytic.javaspark.sparksql;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

public class SimpleFeatureMapper implements
		Function<SimpleFeature, Row>
{
	private static Logger LOGGER = LoggerFactory.getLogger(SimpleFeatureDataFrame.class);

	private final StructType schema;

	public SimpleFeatureMapper(
			StructType schema ) {
		this.schema = schema;
	}

	@Override
	public Row call(
			SimpleFeature feature )
			throws Exception {
		Object[] fields = new Serializable[schema.size()];

		for (int i = 0; i < schema.size(); i++) {
			StructField structField = schema.apply(i);
			if (structField.name().equals(
					"geom")) {
				fields[i] = GeometrySerializer.encode((Geometry) feature.getAttribute(i));
			}
			else if (structField.dataType() == DataTypes.TimestampType) {
				fields[i] = ((Timestamp) new Timestamp(
						((Date) feature.getAttribute(i)).getTime()));
			}
			else if (structField.dataType() != null) {
				fields[i] = (Serializable) feature.getAttribute(i);
			}
			else {
				LOGGER.error("Unexpected attribute in field(" + structField.name() + "): " + feature.getAttribute(i));
			}
		}

		return RowFactory.create(fields);
	}

}
