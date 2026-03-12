package extendableHashing;

final class ExtendableHashFileFormat {
    static final int MAGIC = 0x45585448;
    static final int VERSION = 1;

    static final int MAX_GLOBAL_DEPTH = 20;
    static final int DIRECTORY_CAPACITY = 1 << MAX_GLOBAL_DEPTH;

    static final int HEADER_SIZE = 64;
    static final int OFF_MAGIC = 0;
    static final int OFF_VERSION = 4;
    static final int OFF_BUCKET_CAPACITY = 8;
    static final int OFF_GLOBAL_DEPTH = 12;
    static final int OFF_HASH_A = 16;
    static final int OFF_HASH_B = 20;
    static final int OFF_NEXT_FREE = 24;
    static final int OFF_MAX_GLOBAL_DEPTH = 32;

    static final long DIRECTORY_OFFSET = HEADER_SIZE;
    static final long DIRECTORY_BYTES = (long) DIRECTORY_CAPACITY * Long.BYTES;
    static final long BUCKET_REGION_OFFSET = DIRECTORY_OFFSET + DIRECTORY_BYTES;
    static final long MIN_FILE_SIZE = BUCKET_REGION_OFFSET + 4096;

    static final int BUCKET_OFF_LOCAL_DEPTH = 0;
    static final int BUCKET_OFF_SIZE = 4;
    static final int BUCKET_HEADER_SIZE = 8;

    private ExtendableHashFileFormat() {
    }

    static int bucketRecordSizeFor(int bucketCapacity) {
        return BUCKET_HEADER_SIZE + (bucketCapacity * Integer.BYTES * 2);
    }

    static long directoryOffset(int directoryIndex) {
        return DIRECTORY_OFFSET + ((long) directoryIndex * Long.BYTES);
    }

    static long bucketKeysOffset(long bucketOffset) {
        return bucketOffset + BUCKET_HEADER_SIZE;
    }

    static long bucketValuesOffset(long bucketOffset, int bucketCapacity) {
        return bucketKeysOffset(bucketOffset) + ((long) bucketCapacity * Integer.BYTES);
    }
}
