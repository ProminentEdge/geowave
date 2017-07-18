package mil.nga.giat.geowave.analytic.javaspark.sparksql.udt;

public class PointUDT extends
		GeometryUDT<PointWrapper>
{
	@Override
	public Class userClass() {
		return PointUDT.class;
	}

}
