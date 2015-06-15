package mil.nga.giat.geowave.analytic.partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * Provide a partition for a data item.
 *
 *
 * Multiple partitions are permitted. Only one partition is consider primary. A
 * primary partition is the partition for an item in which the item is processed
 * on behalf of itself. All other partitions are those partitions that require
 * visibility to the a specific item for other items to reference. This approach
 * supports nearest neighbor type queries. Consider that an item can only
 * discover neighbors in its partition. However, the item can be discovered as a
 * nearest neighbor in those partitions in which the item participates as a none
 * primary.
 *
 * @param <T>
 */
public interface Partitioner<T>
{

	public void initialize(
			final JobContext context,
			final Class<?> scope )
					throws IOException;

	public List<PartitionData> getCubeIdentifiers(
			final T entry );

	public void fillOptions(
			Set<Option> options );

	public void setup(
			PropertyManagement runTimeProperties,
			Class<?> scope,
			Configuration configuration );

	public static class PartitionData implements
			Serializable,
			Writable
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		private ByteArrayId id;
		private boolean isPrimary;

		public ByteArrayId getId() {
			return id;
		}

		public boolean isPrimary() {
			return isPrimary;
		}

		public PartitionData() {}

		public PartitionData(
				final ByteArrayId id,
				final boolean primary ) {
			super();
			this.id = id;
			this.isPrimary = primary;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((id == null) ? 0 : id.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final PartitionData other = (PartitionData) obj;
			if (id == null) {
				if (other.id != null) {
					return false;
				}
			}
			else if (!id.equals(
					other.id)) {
				return false;
			}
			return true;
		}

		@Override
		public void readFields(
				final DataInput dInput )
						throws IOException {
			final int idSize = dInput.readInt();
			final byte[] idBytes = new byte[idSize];
			dInput.readFully(
					idBytes);
			isPrimary = dInput.readBoolean();
			id = new ByteArrayId(
					idBytes);
		}

		@Override
		public void write(
				final DataOutput dOutput )
						throws IOException {
			final byte[] outputId = id.getBytes();
			dOutput.writeInt(
					outputId.length);
			dOutput.write(
					outputId);
			dOutput.writeBoolean(
					isPrimary);

		}
	}

}
