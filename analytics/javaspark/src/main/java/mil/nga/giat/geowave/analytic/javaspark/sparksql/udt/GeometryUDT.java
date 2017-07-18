package mil.nga.giat.geowave.analytic.javaspark.sparksql.udt;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.unsafe.types.UTF8String;
import org.geotools.geometry.jts.WKTReader2;
import org.geotools.geometry.jts.WKTWriter2;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;

public abstract class GeometryUDT<T extends Geometry> extends
		UserDefinedType<T>
{
	protected static StructType dataType;

	private static WKTReader2 wktReader = new WKTReader2();
	private static WKTWriter2 wktWriter = new WKTWriter2();

	public GeometryUDT() {
		dataType = new StructType();
		dataType.add(
				new StructField(
						"geom",
						DataTypes.StringType,
						false,
						null));
	}

	@Override
	public T deserialize(
			Object obj ) {
		if (obj instanceof InternalRow) {
			T geomWrapper = null;

			InternalRow row = (InternalRow) obj;

			String geomString = (String) row.get(
					0,
					DataTypes.StringType);

			try {
				Geometry geom = (T) wktReader.read(
						geomString);

				if (geom instanceof Point) {
					geomWrapper = (T) new PointWrapper(
							(Point) geom);
					return geomWrapper;
				}
			}
			catch (ParseException e) {
				e.printStackTrace();
			}

			if (geomWrapper != null) {
				return geomWrapper;
			}
		}

		throw new IllegalStateException(
				"Unsupported conversion");
	}

	@Override
	public InternalRow serialize(
			T geom ) {
		String geomString = wktWriter.write(
				geom);

		InternalRow row = new GenericInternalRow(
				dataType.size());

		row.update(
				0,
				UTF8String.fromString(
						geomString));

		return row;
	}

	@Override
	public DataType sqlType() {
		return dataType;
	}
}
