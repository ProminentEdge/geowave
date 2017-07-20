package mil.nga.giat.geowave.analytic.javaspark.sparksql.udf;

import org.apache.spark.sql.api.java.UDF2;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.javaspark.sparksql.SimpleFeatureMapper;

public class GFContains implements
		UDF2<byte[], byte[], Boolean>
{
	@Override
	public Boolean call(
			byte[] geomString1,
			byte[] geomString2 )
			throws Exception {
		Geometry geom1 = SimpleFeatureMapper.wkbReader.read(
				geomString1);
		Geometry geom2 = SimpleFeatureMapper.wkbReader.read(
				geomString2);

		return geom1.contains(
				geom2);
	}
}
