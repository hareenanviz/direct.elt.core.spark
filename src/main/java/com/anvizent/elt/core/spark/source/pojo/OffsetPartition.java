package com.anvizent.elt.core.spark.source.pojo;

import java.io.Serializable;

import org.apache.spark.Partition;

public class OffsetPartition implements Partition, Serializable {
	private static final long serialVersionUID = 1L;

	private int index;
	private long offset;
	private long size;

	public OffsetPartition(int index, long offset, long size) {
		this.index = index;
		this.offset = offset;
		this.size = size;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	@Override
	public int index() {
		return index;
	}

	public static void main(String[] args) throws Exception {
		Object[][] data2 = { { 20l, 1, new long[][] { { 0, 20 } } }, { 20l, 2, new long[][] { { 0, 10 }, { 10, 10 } } },
		        { 20l, 3, new long[][] { { 0, 7 }, { 7, 7 }, { 14, 6 } } }, { 20l, 4, new long[][] { { 0, 5 }, { 5, 5 }, { 10, 5 }, { 15, 5 } } },
		        { 20l, 5, new long[][] { { 0, 4 }, { 4, 4 }, { 8, 4 }, { 12, 4 }, { 16, 4 } } },
		        { 20l, 6, new long[][] { { 0, 4 }, { 4, 4 }, { 8, 3 }, { 11, 3 }, { 14, 3 }, { 17, 3 } } },
		        { 20l, 7, new long[][] { { 0, 3 }, { 3, 3 }, { 6, 3 }, { 9, 3 }, { 12, 3 }, { 15, 3 }, { 18, 2 } } },
		        { 20l, 8, new long[][] { { 0, 3 }, { 3, 3 }, { 6, 3 }, { 9, 3 }, { 12, 2 }, { 14, 2 }, { 16, 2 }, { 18, 2 } } },
		        { 20l, 9, new long[][] { { 0, 3 }, { 3, 3 }, { 6, 2 }, { 8, 2 }, { 10, 2 }, { 12, 2 }, { 14, 2 }, { 16, 2 }, { 18, 2 } } },
		        { 20l, 10, new long[][] { { 0, 2 }, { 2, 2 }, { 4, 2 }, { 6, 2 }, { 8, 2 }, { 10, 2 }, { 12, 2 }, { 14, 2 }, { 16, 2 }, { 18, 2 } } },
		        { 20l, 11,
		                new long[][] { { 0, 2 }, { 2, 2 }, { 4, 2 }, { 6, 2 }, { 8, 2 }, { 10, 2 }, { 12, 2 }, { 14, 2 }, { 16, 2 }, { 18, 1 }, { 19, 1 } } },
		        { 20l, 12,
		                new long[][] { { 0, 2 }, { 2, 2 }, { 4, 2 }, { 6, 2 }, { 8, 2 }, { 10, 2 }, { 12, 2 }, { 14, 2 }, { 16, 1 }, { 17, 1 }, { 18, 1 },
		                        { 19, 1 } } },
		        { 20l, 13,
		                new long[][] { { 0, 2 }, { 2, 2 }, { 4, 2 }, { 6, 2 }, { 8, 2 }, { 10, 2 }, { 12, 2 }, { 14, 1 }, { 15, 1 }, { 16, 1 }, { 17, 1 },
		                        { 18, 1 }, { 19, 1 } } },
		        { 20l, 14,
		                new long[][] { { 0, 2 }, { 2, 2 }, { 4, 2 }, { 6, 2 }, { 8, 2 }, { 10, 2 }, { 12, 1 }, { 13, 1 }, { 14, 1 }, { 15, 1 }, { 16, 1 },
		                        { 17, 1 }, { 18, 1 }, { 19, 1 } } },
		        { 20l, 15,
		                new long[][] { { 0, 2 }, { 2, 2 }, { 4, 2 }, { 6, 2 }, { 8, 2 }, { 10, 1 }, { 11, 1 }, { 12, 1 }, { 13, 1 }, { 14, 1 }, { 15, 1 },
		                        { 16, 1 }, { 17, 1 }, { 18, 1 }, { 19, 1 } } },
		        { 20l, 16,
		                new long[][] { { 0, 2 }, { 2, 2 }, { 4, 2 }, { 6, 2 }, { 8, 1 }, { 9, 1 }, { 10, 1 }, { 11, 1 }, { 12, 1 }, { 13, 1 }, { 14, 1 },
		                        { 15, 1 }, { 16, 1 }, { 17, 1 }, { 18, 1 }, { 19, 1 } } },
		        { 20l, 17,
		                new long[][] { { 0, 2 }, { 2, 2 }, { 4, 2 }, { 6, 1 }, { 7, 1 }, { 8, 1 }, { 9, 1 }, { 10, 1 }, { 11, 1 }, { 12, 1 }, { 13, 1 },
		                        { 14, 1 }, { 15, 1 }, { 16, 1 }, { 17, 1 }, { 18, 1 }, { 19, 1 } } },
		        { 20l, 18,
		                new long[][] { { 0, 2 }, { 2, 2 }, { 4, 1 }, { 5, 1 }, { 6, 1 }, { 7, 1 }, { 8, 1 }, { 9, 1 }, { 10, 1 }, { 11, 1 }, { 12, 1 },
		                        { 13, 1 }, { 14, 1 }, { 15, 1 }, { 16, 1 }, { 17, 1 }, { 18, 1 }, { 19, 1 } } },
		        { 20l, 19,
		                new long[][] { { 0, 2 }, { 2, 1 }, { 3, 1 }, { 4, 1 }, { 5, 1 }, { 6, 1 }, { 7, 1 }, { 8, 1 }, { 9, 1 }, { 10, 1 }, { 11, 1 },
		                        { 12, 1 }, { 13, 1 }, { 14, 1 }, { 15, 1 }, { 16, 1 }, { 17, 1 }, { 18, 1 }, { 19, 1 } } },
		        { 20l, 20, new long[][] { { 0, 1 }, { 1, 1 }, { 2, 1 }, { 3, 1 }, { 4, 1 }, { 5, 1 }, { 6, 1 }, { 7, 1 }, { 8, 1 }, { 9, 1 }, { 10, 1 },
		                { 11, 1 }, { 12, 1 }, { 13, 1 }, { 14, 1 }, { 15, 1 }, { 16, 1 }, { 17, 1 }, { 18, 1 }, { 19, 1 } } } };

		Object[][] data = {
		        { 20l, 1l,
		                new long[][] { { 0, 1 }, { 1, 1 }, { 2, 1 }, { 3, 1 }, { 4, 1 }, { 5, 1 }, { 6, 1 }, { 7, 1 }, { 8, 1 }, { 9, 1 }, { 10, 1 }, { 11, 1 },
		                        { 12, 1 }, { 13, 1 }, { 14, 1 }, { 15, 1 }, { 16, 1 }, { 17, 1 }, { 18, 1 }, { 19, 1 } } },
		        { 20l, 4l, new long[][] { { 0, 4 }, { 4, 4 }, { 8, 4 }, { 12, 4 }, { 16, 4 } } },
		        { 20l, 5l, new long[][] { { 0, 5 }, { 5, 5 }, { 10, 5 }, { 15, 5 } } }, { 20l, 6l, new long[][] { { 0, 6 }, { 6, 6 }, { 12, 6 }, { 18, 2 } } },
		        { 20l, 7l, new long[][] { { 0, 7 }, { 7, 7 }, { 14, 6 } } }, { 20l, 8l, new long[][] { { 0, 8 }, { 8, 8 }, { 16, 4 } } },
		        { 20l, 9l, new long[][] { { 0, 9 }, { 9, 9 }, { 18, 2 } } }, { 20l, 10l, new long[][] { { 0, 10 }, { 10, 10 } } },
		        { 20l, 11l, new long[][] { { 0, 11 }, { 11, 9 } } }, { 20l, 12l, new long[][] { { 0, 12 }, { 12, 8 } } },
		        { 20l, 13l, new long[][] { { 0, 13 }, { 13, 7 } } }, { 20l, 14l, new long[][] { { 0, 14 }, { 14, 6 } } },
		        { 20l, 15l, new long[][] { { 0, 15 }, { 15, 5 } } }, { 20l, 16l, new long[][] { { 0, 16 }, { 16, 4 } } },
		        { 20l, 17l, new long[][] { { 0, 17 }, { 17, 3 } } }, { 20l, 18l, new long[][] { { 0, 18 }, { 18, 2 } } },
		        { 20l, 19l, new long[][] { { 0, 19 }, { 19, 1 } } }, { 20l, 20l, new long[][] { { 0, 20 } } } };

		System.out.println(data2);

		for (int i = 0; i < data.length; i++) {
			validate(data[i], i);
		}
	}

	private static void validate(Object[] objects, int i) throws Exception {
		OffsetPartition[] partitions = OffsetPartition.getPartitions((long) objects[0], (long) objects[1]);
		validate(partitions, (long[][]) objects[2], i);
	}

	private static void validate(OffsetPartition[] partitions, long[][] offsets, int i) throws Exception {
		for (int j = 0; j < partitions.length; j++) {
			if (partitions[j].offset != offsets[j][0] || partitions[j].size != offsets[j][1]) {
				throw new Exception("i: " + i + ", j: " + j);
			}
		}
	}

	public static OffsetPartition[] getPartitions(long tableSize, long partitionSize) {
		if (tableSize == 0) {
			return getZeroPratition();
		}

		OffsetPartition[] partitions = new OffsetPartition[(int) Math.ceil(((double) tableSize) / ((double) partitionSize))];
		int i;

		for (i = 0; i < partitions.length - 1; i++) {
			partitions[i] = new OffsetPartition(i, i * partitionSize, partitionSize);
		}

		partitions[i] = new OffsetPartition(i, i * partitionSize, tableSize - i * partitionSize);

		return partitions;
	}

	private static OffsetPartition[] getZeroPratition() {
		return new OffsetPartition[] { new OffsetPartition(0, 0, 0) };
	}

	private static OffsetPartition[] getZeroPratition(int numberOfPartition) {
		OffsetPartition[] partitions = new OffsetPartition[numberOfPartition];

		for (int i = 0; i < partitions.length; i++) {
			partitions[i] = new OffsetPartition(i, 0, 0);
		}

		return partitions;
	}

	public static OffsetPartition[] getPartitions(long tableSize, int numberOfPartition) {
		if (tableSize == 0) {
			return getZeroPratition(numberOfPartition);
		}

		OffsetPartition[] partitions = new OffsetPartition[numberOfPartition];
		long remaining = tableSize % numberOfPartition;
		long reminder = tableSize % numberOfPartition;
		long partitionSize = tableSize / numberOfPartition;

		for (int i = 0; i < partitions.length; i++) {
			if (remaining > 0) {
				partitions[i] = new OffsetPartition(i, i * (partitionSize + 1), partitionSize + 1);
				remaining--;
			} else {
				partitions[i] = new OffsetPartition(i, i * partitionSize + reminder, partitionSize);
			}
		}

		return partitions;
	}
}
