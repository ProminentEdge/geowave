package mil.nga.giat.geowave.core.cli;

import java.util.Map;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class IndexStoreCommandLineOptions extends
		GenericStoreCommandLineOptions<IndexStore>
{
	public IndexStoreCommandLineOptions(
			final GenericStoreFactory<IndexStore> factory,
			final Map<String, Object> configOptions,
			final String namespace ) {
		super(
				factory,
				configOptions,
				namespace);
	}

	public static void applyOptions(
			final Options allOptions ) {
		applyOptions(
				allOptions,
				new IndexStoreCommandLineHelper());
	}

	public static IndexStoreCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		return (IndexStoreCommandLineOptions) parseOptions(
				commandLine,
				new IndexStoreCommandLineHelper());
	}

	@Override
	public IndexStore createStore() {
		return GeoWaveStoreFinder.createIndexStore(
				configOptions,
				namespace);
	}

	private static class IndexStoreCommandLineHelper implements
			CommandLineHelper<IndexStore, IndexStoreFactorySpi>
	{
		@Override
		public Map<String, IndexStoreFactorySpi> getRegisteredFactories() {
			return GeoWaveStoreFinder.getRegisteredIndexStoreFactories();
		}

		@Override
		public String getOptionName() {
			return "indexstore";
		}

		@Override
		public GenericStoreCommandLineOptions<IndexStore> createCommandLineOptions(
				final GenericStoreFactory<IndexStore> factory,
				final Map<String, Object> configOptions,
				final String namespace ) {
			return new IndexStoreCommandLineOptions(
					factory,
					configOptions,
					namespace);
		}
	}
}