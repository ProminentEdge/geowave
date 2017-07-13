package mil.nga.giat.geowave.analytic.javaspark.sparksql.udt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.SQLUserDefinedType;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

@SQLUserDefinedType(udt = FeatureUDT.class)
public class Feature implements
		Serializable
{
	private Geometry geom;
	private List<Serializable> attributes = new ArrayList<>();
	
	protected Feature() {}

	public Feature(
			final Geometry geom,
			final List<Serializable> attributes ) {
		this.geom = geom;
		this.attributes.addAll(
				attributes);
	}

	public Feature(
			final SimpleFeature simpleFeature ) {
		for (Object attr : simpleFeature.getAttributes()) {
			if (attr instanceof Geometry) {
				this.geom = (Geometry) attr;
			}
			else if (attr instanceof Serializable) {
				attributes.add(
						(Serializable) attr);
			}
			else {
				// TODO: ?
			}
		}
	}

	public Geometry getGeom() {
		return geom;
	}

	public void setGeom(
			Geometry geom ) {
		this.geom = geom;
	}

	public List<Serializable> getAttributes() {
		return attributes;
	}

	public void setAttributes(
			List<Serializable> attributes ) {
		this.attributes = attributes;
	}
}
