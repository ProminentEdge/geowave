package mil.nga.giat.geowave.analytic.javaspark.sparksql.udt;

import org.apache.spark.sql.types.SQLUserDefinedType;

import com.vividsolutions.jts.geom.Point;

@SQLUserDefinedType(udt = PointUDT.class)
public class PointWrapper extends
		Point
{
	public PointWrapper(
			Point point ) {
		super(
				point.getCoordinateSequence(),
				point.getFactory());
	}
}
