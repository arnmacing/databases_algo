package extendableHashing;

final class ExtendableHashFileFormat {
    static final int MAX_GLOBAL_DEPTH = 16;
    static final int DIRECTORY_CAPACITY = 1 << MAX_GLOBAL_DEPTH;
    static final int OFF_BUCKET_CAPACITY = 0;
    static final int OFF_GLOBAL_DEPTH = OFF_BUCKET_CAPACITY + Integer.BYTES;
    static final int OFF_NEXT_FREE = OFF_GLOBAL_DEPTH + Integer.BYTES;
    static final int HEADER_SIZE = OFF_NEXT_FREE + Long.BYTES;
    static final long DIRECTORY_OFFSET = HEADER_SIZE;
    static final long DIRECTORY_BYTES = (long) DIRECTORY_CAPACITY * Long.BYTES;
    static final long BUCKET_REGION_OFFSET = DIRECTORY_OFFSET + DIRECTORY_BYTES;
    static final long MIN_FILE_SIZE = BUCKET_REGION_OFFSET + 4096L;
    static final int BUCKET_OFF_LOCAL_DEPTH = 0;
    static final int BUCKET_OFF_SIZE = 4;
    static final int BUCKET_HEADER_SIZE = 8;

    private ExtendableHashFileFormat() {
    }

    /**
     * Размер одной бакета в байтах
     */
    static int bucketRecordSizeFor(int bucketCapacity) {
        return BUCKET_HEADER_SIZE + (bucketCapacity * Integer.BYTES * 2);
    }

    /**
     * Смещение указателя нужной ячейки каталога
     */
    static long directoryOffset(int directoryIndex) {
        return DIRECTORY_OFFSET + ((long) directoryIndex * Long.BYTES);
    }

    /**
     * Смещение начала области ключей внутри бакета
     */
    static long bucketKeysOffset(long bucketOffset) {
        return bucketOffset + BUCKET_HEADER_SIZE;
    }

    /**
     * Смещение начала области значений внутри бакета
     */
    static long bucketValuesOffset(long bucketOffset, int bucketCapacity) {
        return bucketKeysOffset(bucketOffset) + ((long) bucketCapacity * Integer.BYTES);
    }
}
