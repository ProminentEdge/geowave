package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.base.Writer;

public class DynamoDBWriter implements
		Writer<WriteRequest>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(
			DynamoDBWriter.class);
	private static final int NUM_ITEMS = 25;

	private final List<WriteRequest> batchedItems = new ArrayList<>();
	private final String tableName;
	private final AmazonDynamoDBAsyncClient client;

	public DynamoDBWriter(
			final String tableName,
			final AmazonDynamoDBAsyncClient client ) {
		this.tableName = tableName;
		this.client = client;
	}

	@Override
	public void close()
			throws IOException {
		flush();
	}

	@Override
	public void write(
			final Iterable<WriteRequest> items ) {
		for (final WriteRequest item : items) {
			write(
					item);
		}
	}

	@Override
	public void write(
			final WriteRequest item ) {
		synchronized (batchedItems) {
			batchedItems.add(
					item);
			if (batchedItems.size() >= NUM_ITEMS) {
				do {
					writeBatch(
							true);
				}
				while (batchedItems.size() >= NUM_ITEMS);
			}
		}
	}

	private void writeBatch(
			final boolean async ) {
		final List<WriteRequest> batch;

		if (batchedItems.size() <= NUM_ITEMS) {
			batch = batchedItems;
		}
		else {
			batch = batchedItems.subList(
					0,
					NUM_ITEMS + 1);
		}
		final Map<String, List<WriteRequest>> writes = new HashMap<>();
		writes.put(
				tableName,
				new ArrayList<>(
						batch));
		// if (async) {
		// final Future<BatchWriteItemResult> response =
		// client.batchWriteItemAsync(
		// new BatchWriteItemRequest(
		// writes));
		//
		// DynamoDBClientPool.DYNAMO_RETRY_POOL.execute(
		// new Runnable() {
		// @Override
		// public void run() {
		// try {
		// final Map<String, List<WriteRequest>> map =
		// response.get().getUnprocessedItems();
		// retry(
		// map);
		// }
		// catch (InterruptedException | ExecutionException e) {
		// LOGGER.warn(
		// "Unable to get response from Async Write",
		// e);
		// }
		// }
		// });
		// }
		// else {
		Set<AttributeValue> rs = new HashSet<>();
		for (List<WriteRequest> wl : writes.values()){
			for (WriteRequest w :wl){
//				AttributeValue v1 = w.getPutRequest().getItem().get(DynamoDBRow.GW_PARTITION_ID_KEY);
				AttributeValue v2 = w.getPutRequest().getItem().get(DynamoDBRow.GW_RANGE_KEY);
				if(!rs.add(v2)){
					byte[] v2b = v2.getB().array();
					
					System.err.println("crap" + new ByteArrayId(v2b).getHexString() + "  " + new String(v2b));
				}
			}
		}
		final BatchWriteItemResult response = client.batchWriteItem(
				new BatchWriteItemRequest(
						writes));
		retry(
				response.getUnprocessedItems());
		// }
		batch.clear();
	}

	private void retry(
			final Map<String, List<WriteRequest>> map ) {
		for (final Entry<String, List<WriteRequest>> requests : map.entrySet()) {
			for (final WriteRequest r : requests.getValue()) {
				if (r.getPutRequest() != null) {
					client.putItem(
							requests.getKey(),
							r.getPutRequest().getItem());
				}
			}
		}
	}

	@Override
	public void flush() {
		synchronized (batchedItems) {
			do {
				writeBatch(
						false);
			}
			while (!batchedItems.isEmpty());
		}
	}

}