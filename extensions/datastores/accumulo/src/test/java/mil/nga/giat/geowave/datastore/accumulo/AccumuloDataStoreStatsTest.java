package mil.nga.giat.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.FieldTypeStatisticVisibility;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class AccumuloDataStoreStatsTest
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloDataStoreStatsTest.class);

	final AccumuloOptions accumuloOptions = new AccumuloOptions();

	final GeometryFactory factory = new GeometryFactory();

	AccumuloOperations accumuloOperations;

	AccumuloIndexStore indexStore;

	AccumuloAdapterStore adapterStore;

	AccumuloDataStatisticsStore statsStore;

	AccumuloDataStore mockDataStore;

	AccumuloSecondaryIndexDataStore secondaryIndexDataStore;

	@Before
	public void setUp() {
		final MockInstance mockInstance = new MockInstance();
		Connector mockConnector = null;
		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Failed to create mock accumulo connection",
					e);
		}
		accumuloOperations = new BasicAccumuloOperations(
				mockConnector);

		indexStore = new AccumuloIndexStore(
				accumuloOperations);

		adapterStore = new AccumuloAdapterStore(
				accumuloOperations);

		statsStore = new AccumuloDataStatisticsStore(
				accumuloOperations);

		secondaryIndexDataStore = new AccumuloSecondaryIndexDataStore(
				accumuloOperations);

		mockDataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				statsStore,
				secondaryIndexDataStore,
				accumuloOperations,
				accumuloOptions);
	}

	public static final VisibilityWriter<TestGeometry> visWriterAAA = new VisibilityWriter<TestGeometry>() {

		@Override
		public FieldVisibilityHandler<TestGeometry, Object> getFieldVisibilityHandler(
				final ByteArrayId fieldId ) {
			return new FieldVisibilityHandler<TestGeometry, Object>() {
				@Override
				public byte[] getVisibility(
						final TestGeometry rowValue,
						final ByteArrayId fieldId,
						final Object fieldValue ) {
					return "aaa".getBytes();
				}

			};
		}

	};

	public static final VisibilityWriter<TestGeometry> visWriterBBB = new VisibilityWriter<TestGeometry>() {

		@Override
		public FieldVisibilityHandler<TestGeometry, Object> getFieldVisibilityHandler(
				final ByteArrayId fieldId ) {
			return new FieldVisibilityHandler<TestGeometry, Object>() {
				@Override
				public byte[] getVisibility(
						final TestGeometry rowValue,
						final ByteArrayId fieldId,
						final Object fieldValue ) {
					return "bbb".getBytes();
				}

			};
		}

	};

	@Test
	public void testWithOutAltIndex()
			throws IOException {
		accumuloOptions.setCreateTable(true);
		accumuloOptions.setUseAltIndex(false);
		accumuloOptions.setPersistDataStatistics(true);
		runtest();
	}

	@Test
	public void testWithAltIndex()
			throws IOException {
		accumuloOptions.setCreateTable(true);
		accumuloOptions.setUseAltIndex(true);
		accumuloOptions.setPersistDataStatistics(true);
		runtest();
	}

	private void runtest()
			throws IOException {

		final PrimaryIndex index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		final Geometry testGeoFilter = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					24,
					33),
			new Coordinate(
					28,
					33),
			new Coordinate(
					28,
					31),
			new Coordinate(
					24,
					31),
			new Coordinate(
					24,
					33)
		});

		ByteArrayId rowId0, rowId1;
		try (IndexWriter indexWriter = mockDataStore.createIndexWriter(
				index,
				DataStoreUtils.DEFAULT_VISIBILITY)) {
			rowId0 = indexWriter.write(
					adapter,
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt"),
					visWriterAAA).get(
					0);
			rowId1 = indexWriter.write(
					adapter,
					new TestGeometry(
							factory.createPoint(new Coordinate(
									26,
									32)),
							"test_pt_1"),
					visWriterAAA).get(
					0);
			indexWriter.write(
					adapter,
					new TestGeometry(
							factory.createPoint(new Coordinate(
									27,
									32)),
							"test_pt_2"),
					visWriterBBB).get(
					0);
		}

		final SpatialQuery query = new SpatialQuery(
				testGeoFilter);

		CloseableIterator it1 = mockDataStore.query(
				new QueryOptions(
						Collections.<String> emptyList(),
						adapter,
						index,
						-1,
						null,
						new String[] {
							"aaa",
							"bbb"
						}),
				query);
		int count = 0;
		while (it1.hasNext()) {
			it1.next();
			count++;
		}
		assertEquals(
				3,
				count);

		CountDataStatistics countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"aaa",
				"bbb");
		assertEquals(
				3,
				countStats.getCount());

		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"aaa");
		assertEquals(
				2,
				countStats.getCount());

		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"bbb");
		assertEquals(
				1,
				countStats.getCount());

		BoundingBoxDataStatistics bboxStats = (BoundingBoxDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				BoundingBoxDataStatistics.STATS_ID,
				"aaa");
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 26) && (bboxStats.getMinY() == 32) && (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				BoundingBoxDataStatistics.STATS_ID,
				"bbb");
		assertTrue((bboxStats.getMinX() == 27) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32) && (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				BoundingBoxDataStatistics.STATS_ID,
				"aaa",
				"bbb");
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32) && (bboxStats.getMaxY() == 32));

		final AtomicBoolean found = new AtomicBoolean(
				false);
		mockDataStore.delete(
				new QueryOptions(
						Collections.<String> emptyList(),
						adapter,
						index,
						-1,
						new ScanCallback<TestGeometry>() {

							@Override
							public void entryScanned(
									DataStoreEntryInfo entryInfo,
									TestGeometry entry ) {
								found.getAndSet(true);
							}
						},
						new String[] {
							"aaa"
						}),
				new DataIdQuery(
						adapter.getAdapterId(),
						new ByteArrayId(
								"test_pt_2".getBytes(StringUtils.UTF8_CHAR_SET))));
		assertFalse(found.get());

		it1 = mockDataStore.query(
				new QueryOptions(
						Collections.<String> emptyList(),
						adapter,
						index,
						-1,
						null,
						new String[] {
							"aaa",
							"bbb"
						}),
				query);
		count = 0;
		while (it1.hasNext()) {
			it1.next();
			count++;
		}
		assertEquals(
				3,
				count);

		mockDataStore.delete(
				new QueryOptions(
						Collections.<String> emptyList(),
						adapter,
						index,
						-1,
						null,
						new String[] {
							"aaa"
						}),
				new DataIdQuery(
						adapter.getAdapterId(),
						new ByteArrayId(
								"test_pt".getBytes(StringUtils.UTF8_CHAR_SET))));

		it1 = mockDataStore.query(
				new QueryOptions(
						Collections.<String> emptyList(),
						adapter,
						index,
						-1,
						null,
						new String[] {
							"aaa",
							"bbb"
						}),
				query);
		count = 0;
		while (it1.hasNext()) {
			it1.next();
			count++;
		}
		assertEquals(
				2,
				count);

		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"aaa");
		assertEquals(
				1,
				countStats.getCount());

		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"bbb");
		assertEquals(
				1,
				countStats.getCount());

		bboxStats = (BoundingBoxDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				BoundingBoxDataStatistics.STATS_ID,
				"aaa");
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 26) && (bboxStats.getMinY() == 32) && (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				BoundingBoxDataStatistics.STATS_ID,
				"bbb");
		assertTrue((bboxStats.getMinX() == 27) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32) && (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				BoundingBoxDataStatistics.STATS_ID,
				"aaa",
				"bbb");
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32) && (bboxStats.getMaxY() == 32));

		found.set(false);

		assertTrue(mockDataStore.delete(
				new QueryOptions(
						Collections.<String> emptyList(),
						adapter,
						index,
						-1,
						new ScanCallback<TestGeometry>() {

							@Override
							public void entryScanned(
									DataStoreEntryInfo entryInfo,
									TestGeometry entry ) {
								found.getAndSet(true);
							}
						},
						new String[] {
							"aaa",
							"bbb"
						}),
				new EverythingQuery()));

		it1 = mockDataStore.query(
				new QueryOptions(
						Collections.<String> emptyList(),
						adapter,
						index,
						-1,
						null,
						new String[] {
							"aaa",
							"bbb"
						}),
				query);
		count = 0;
		while (it1.hasNext()) {
			it1.next();
			count++;
		}
		assertEquals(
				0,
				count);

		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID);
		assertNull(countStats);

		try (IndexWriter indexWriter = mockDataStore.createIndexWriter(
				index,
				visWriterBBB)) {
			rowId0 = indexWriter.write(
					adapter,
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_2")).get(
					0);
		}

		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"bbb");
		assertTrue(countStats != null);

		statsStore.deleteObjects(
				adapter.getAdapterId(),
				"bbb");

		countStats = (CountDataStatistics) statsStore.getDataStatistics(
				adapter.getAdapterId(),
				CountDataStatistics.STATS_ID,
				"bbb");
		assertNull(countStats);

		RowRangeDataStatistics rowStats = (RowRangeDataStatistics) statsStore.getDataStatistics(
				null,
				RowRangeDataStatistics.getId(index.getId()),
				"bbb");

		assertTrue(rowStats != null);

	}

	protected static class TestGeometry
	{
		private final Geometry geom;
		private final String id;

		public TestGeometry(
				final Geometry geom,
				final String id ) {
			this.geom = geom;
			this.id = id;
		}
	}

	protected static class TestGeometryAdapter extends
			AbstractDataAdapter<TestGeometry> implements
			StatisticalDataAdapter<TestGeometry>
	{
		private static final ByteArrayId GEOM = new ByteArrayId(
				"myGeo");
		private static final ByteArrayId ID = new ByteArrayId(
				"myId");

		private static final PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object> GEOM_FIELD_HANDLER = new PersistentIndexFieldHandler<TestGeometry, CommonIndexValue, Object>() {

			@Override
			public ByteArrayId[] getNativeFieldIds() {
				return new ByteArrayId[] {
					GEOM
				};
			}

			@Override
			public CommonIndexValue toIndexValue(
					final TestGeometry row ) {
				return new GeometryWrapper(
						row.geom,
						new byte[0]);
			}

			@Override
			public PersistentValue<Object>[] toNativeValues(
					final CommonIndexValue indexValue ) {
				return new PersistentValue[] {
					new PersistentValue<Object>(
							GEOM,
							((GeometryWrapper) indexValue).getGeometry())
				};
			}

			@Override
			public byte[] toBinary() {
				return new byte[0];
			}

			@Override
			public void fromBinary(
					final byte[] bytes ) {

			}
		};

		private final static EntryVisibilityHandler<TestGeometry> GEOMETRY_VISIBILITY_HANDLER = new FieldTypeStatisticVisibility<TestGeometry>(
				GeometryWrapper.class);
		private static final NativeFieldHandler<TestGeometry, Object> ID_FIELD_HANDLER = new NativeFieldHandler<TestGeometry, Object>() {

			@Override
			public ByteArrayId getFieldId() {
				return ID;
			}

			@Override
			public Object getFieldValue(
					final TestGeometry row ) {
				return row.id;
			}

		};

		private static final List<NativeFieldHandler<TestGeometry, Object>> NATIVE_FIELD_HANDLER_LIST = new ArrayList<NativeFieldHandler<TestGeometry, Object>>();
		private static final List<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>> COMMON_FIELD_HANDLER_LIST = new ArrayList<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>>();
		static {
			COMMON_FIELD_HANDLER_LIST.add(GEOM_FIELD_HANDLER);
			NATIVE_FIELD_HANDLER_LIST.add(ID_FIELD_HANDLER);
		}

		public TestGeometryAdapter() {
			super(
					COMMON_FIELD_HANDLER_LIST,
					NATIVE_FIELD_HANDLER_LIST);
		}

		@Override
		public ByteArrayId getAdapterId() {
			return new ByteArrayId(
					"test");
		}

		@Override
		public boolean isSupported(
				final TestGeometry entry ) {
			return true;
		}

		@Override
		public ByteArrayId getDataId(
				final TestGeometry entry ) {
			return new ByteArrayId(
					entry.id);
		}

		@Override
		public FieldReader getReader(
				final ByteArrayId fieldId ) {
			if (fieldId.equals(GEOM)) {
				return FieldUtils.getDefaultReaderForClass(Geometry.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultReaderForClass(String.class);
			}
			return null;
		}

		@Override
		public FieldWriter getWriter(
				final ByteArrayId fieldId ) {
			if (fieldId.equals(GEOM)) {
				return FieldUtils.getDefaultWriterForClass(Geometry.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultWriterForClass(String.class);
			}
			return null;
		}

		@Override
		public DataStatistics<TestGeometry> createDataStatistics(
				final ByteArrayId statisticsId ) {
			if (BoundingBoxDataStatistics.STATS_ID.equals(statisticsId)) {
				return new GeoBoundingBoxStatistics(
						getAdapterId());
			}
			else if (CountDataStatistics.STATS_ID.equals(statisticsId)) {
				return new CountDataStatistics<TestGeometry>(
						getAdapterId());
			}
			LOGGER.warn("Unrecognized statistics ID " + statisticsId.getString() + " using count statistic");
			return new CountDataStatistics<TestGeometry>(
					getAdapterId(),
					statisticsId);
		}

		@Override
		public EntryVisibilityHandler<TestGeometry> getVisibilityHandler(
				final ByteArrayId statisticsId ) {
			return GEOMETRY_VISIBILITY_HANDLER;
		}

		@Override
		protected RowBuilder newBuilder() {
			return new RowBuilder<TestGeometry, Object>() {
				private String id;
				private Geometry geom;

				@Override
				public void setField(
						final PersistentValue<Object> fieldValue ) {
					if (fieldValue.getId().equals(
							GEOM)) {
						geom = (Geometry) fieldValue.getValue();
					}
					else if (fieldValue.getId().equals(
							ID)) {
						id = (String) fieldValue.getValue();
					}
				}

				@Override
				public TestGeometry buildRow(
						final ByteArrayId dataId ) {
					return new TestGeometry(
							geom,
							id);
				}
			};
		}

		@Override
		public ByteArrayId[] getSupportedStatisticsIds() {
			return SUPPORTED_STATS_IDS;
		}
	}

	private final static ByteArrayId[] SUPPORTED_STATS_IDS = new ByteArrayId[] {
		BoundingBoxDataStatistics.STATS_ID,
		CountDataStatistics.STATS_ID
	};

	private static class GeoBoundingBoxStatistics extends
			BoundingBoxDataStatistics<TestGeometry>
	{

		protected GeoBoundingBoxStatistics() {
			super();
		}

		public GeoBoundingBoxStatistics(
				final ByteArrayId dataAdapterId ) {
			super(
					dataAdapterId);
		}

		@Override
		protected Envelope getEnvelope(
				final TestGeometry entry ) {
			// incorporate the bounding box of the entry's envelope
			final Geometry geometry = entry.geom;
			if ((geometry != null) && !geometry.isEmpty()) {
				return geometry.getEnvelopeInternal();
			}
			return null;
		}

	}

}