package mil.nga.giat.geowave.analytic.javaspark.sparksql;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

public class GeometrySerializer
{
	private static WKTWriter wktWriter = new WKTWriter();
	private static WKTReader wktReader = new WKTReader();

	public static String encode(
			Geometry geom ) {
		try {
			return wktWriter.write(geom);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public static Geometry decode(
			String geomStr ) {
		try {
			return wktReader.read(geomStr);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

}
