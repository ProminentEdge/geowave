package mil.nga.giat.geowave.analytic.javaspark.sparksql.udt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;
import org.geotools.data.DataUtilities;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;

public class FeatureUDT extends
		UserDefinedType<Feature>
{
	private final StructType dataType;
	
	public FeatureUDT(
			SimpleFeatureType featureType ) {
		this.dataType = schemaFromFeatureType(featureType);
	}

	@Override
	public Feature deserialize(
			Object obj ) {
		if (obj instanceof InternalRow) {
			InternalRow row = (InternalRow) obj;

			Vector geomVector = (Vector) row.get(
					0,
					new VectorUDT());

			Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(
					new Coordinate(
							geomVector.apply(
									0),
							geomVector.apply(
									1)));

			List<Serializable> attributes = new ArrayList<>();
			for (int i = 1; i < dataType.size(); i++) {
				StructField field = dataType.apply(
						i);
				attributes.add(
						(Serializable) row.get(
								i,
								field.dataType()));
			}

			Feature feature = new Feature(
					point,
					attributes);

			return feature;
		}
		
		throw new IllegalStateException(
				"Unsupported conversion");
	}

	@Override
	public InternalRow serialize(
			Feature feature ) {
		InternalRow row = new GenericInternalRow(
				dataType.size());
		Vector geomVector = null;
		Geometry geom = feature.getGeom();
		if (geom instanceof Point) {
			Point point = (Point) geom;
			geomVector = Vectors.dense(
					point.getX(),
					point.getY());
		}

		row.update(
				0,
				geomVector);

		for (int i = 1; i < dataType.size(); i++) {
			row.update(
					i,
					feature.getAttributes().get(
							i - 1));
		}

		return row;
	}

	@Override
	public DataType sqlType() {
		return dataType;
	}

	@Override
	public Class<Feature> userClass() {
		return Feature.class;
	}

	private StructType schemaFromFeatureType(SimpleFeatureType featureType) {
		List<StructField> fields = new ArrayList<>();

		String typeSpec = DataUtilities.encodeType(
				featureType);

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

	private static DataType getDataType(
			String javaType ) {
		// Handle geometry types first (vector of doubles)
		if (javaType.equalsIgnoreCase(
				"point")) {
			return new VectorUDT();
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
