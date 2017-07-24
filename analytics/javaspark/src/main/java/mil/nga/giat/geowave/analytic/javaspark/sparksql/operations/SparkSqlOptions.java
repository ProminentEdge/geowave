package mil.nga.giat.geowave.analytic.javaspark.sparksql.operations;

import com.beust.jcommander.Parameter;

public class SparkSqlOptions
{
	@Parameter(names = {
		"-o",
		"--out"
	}, description = "The output datastore name")
	private String outputStoreName = null;

	public SparkSqlOptions() {}

	public String getOutputStoreName() {
		return outputStoreName;
	}

	public void setOutputStoreName(
			String outputStoreName ) {
		this.outputStoreName = outputStoreName;
	}
}
