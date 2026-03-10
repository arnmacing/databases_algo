package extendableHashing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;

/**
 * Расширяемая хеш-таблица, которая живет в файле с mmap.
 *
 * Как устроен файл:
 * 1) фиксированный заголовок с метаданными таблицы
 * 2) фиксированная область директории (до {@code MAX_GLOBAL_DEPTH})
 * 3) область бакетов, в которую только дописываем
 */
public class ExtendableHashTable implements AutoCloseable {

    // ---- Константы формата файла ----
    private static final int MAGIC = 0x45585448; // EXTH
    private static final int VERSION = 1;

    private static final int MAX_GLOBAL_DEPTH = 20;
    private static final int DIRECTORY_CAPACITY = 1 << MAX_GLOBAL_DEPTH;

    private static final int HEADER_SIZE = 64;
    private static final int OFF_MAGIC = 0;
    private static final int OFF_VERSION = 4;
    private static final int OFF_BUCKET_CAPACITY = 8;
    private static final int OFF_GLOBAL_DEPTH = 12;
    private static final int OFF_HASH_A = 16;
    private static final int OFF_HASH_B = 20;
    private static final int OFF_NEXT_FREE = 24;
    private static final int OFF_MAX_GLOBAL_DEPTH = 32;

    private static final long DIRECTORY_OFFSET = HEADER_SIZE;
    private static final long DIRECTORY_BYTES = (long) DIRECTORY_CAPACITY * Long.BYTES;
    private static final long BUCKET_REGION_OFFSET = DIRECTORY_OFFSET + DIRECTORY_BYTES;
    private static final long MIN_FILE_SIZE = BUCKET_REGION_OFFSET + 4096;

    private static final int BUCKET_OFF_LOCAL_DEPTH = 0;
    private static final int BUCKET_OFF_SIZE = 4;
    private static final int BUCKET_HEADER_SIZE = 8;

    private static final int NOT_FOUND = -1;

    // ---- Неизменяемая конфигурация и ресурсы ----
    private final int bucketCapacity;
    private final int bucketRecordSize;
    private final Path filePath;
    private final boolean deleteOnClose;
    private final FileChannel channel;

    // ---- Кэш состояния из файла ----
    private MappedByteBuffer buffer;
    private long mappedSize;
    private int globalDepth;
    private int hashA;
    private int hashB;
    private long nextFreeOffset;

    // ---- Жизненный цикл ----
    private boolean closed;

    public ExtendableHashTable(int bucketCapacity) {
        this(createTempFile(), bucketCapacity, new SplittableRandom().nextLong(), true);
    }

    public ExtendableHashTable(int bucketCapacity, long seed) {
        this(createTempFile(), bucketCapacity, seed, true);
    }

    public ExtendableHashTable(Path filePath, int bucketCapacity) {
        this(filePath, bucketCapacity, new SplittableRandom().nextLong(), false);
    }

    public ExtendableHashTable(Path filePath, int bucketCapacity, long seed) {
        this(filePath, bucketCapacity, seed, false);
    }

    public static ExtendableHashTable open(Path filePath) {
        return new ExtendableHashTable(filePath);
    }

    private ExtendableHashTable(Path filePath, int bucketCapacity, long seed, boolean deleteOnClose) {
        validateBucketCapacity(bucketCapacity);

        this.bucketCapacity = bucketCapacity;
        this.bucketRecordSize = bucketRecordSizeFor(bucketCapacity);
        this.filePath = filePath;
        this.deleteOnClose = deleteOnClose;

        try {
            this.channel = FileChannel.open(
                    filePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
            this.mappedSize = 0;
            ensureMappedSize(Math.max(MIN_FILE_SIZE, BUCKET_REGION_OFFSET + bucketRecordSize));
            initializeNewTable(seed);
        } catch (IOException e) {
            throw new UncheckedIOException("Не удалось инициализировать расширяемую хеш-таблицу", e);
        }
    }

    private ExtendableHashTable(Path filePath) {
        this.filePath = filePath;
        this.deleteOnClose = false;

        try {
            this.channel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);

            long currentSize = channel.size();
            if (currentSize < BUCKET_REGION_OFFSET) {
                throw new IllegalStateException("Файл таблицы поврежден: слишком маленький");
            }

            this.mappedSize = 0;
            ensureMappedSize(currentSize);

            validateHeader();

            this.bucketCapacity = readHeaderInt(OFF_BUCKET_CAPACITY);
            validateBucketCapacity(bucketCapacity);
            this.bucketRecordSize = bucketRecordSizeFor(bucketCapacity);

            this.globalDepth = readHeaderInt(OFF_GLOBAL_DEPTH);
            if (globalDepth < 0 || globalDepth > MAX_GLOBAL_DEPTH) {
                throw new IllegalStateException("Файл таблицы поврежден: некорректный globalDepth");
            }

            this.hashA = readHeaderInt(OFF_HASH_A);
            this.hashB = readHeaderInt(OFF_HASH_B);
            this.nextFreeOffset = readHeaderLong(OFF_NEXT_FREE);

            if (nextFreeOffset < BUCKET_REGION_OFFSET || nextFreeOffset > mappedSize) {
                throw new IllegalStateException("Файл таблицы поврежден: некорректный nextFreeOffset");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Не удалось открыть расширяемую хеш-таблицу", e);
        }
    }

    // ---- Публичный API ----
    public void put(int key, int value) {
        ensureOpen();

        while (true) {
            int directoryIndex = directoryIndex(key);
            long bucketOffset = readDirectoryPointer(directoryIndex);

            PutResult result = putIntoBucket(bucketOffset, key, value);
            if (result == PutResult.INSERTED || result == PutResult.UPDATED) {
                return;
            }

            splitBucket(directoryIndex);
        }
    }

    public Integer get(int key) {
        ensureOpen();
        long bucketOffset = readDirectoryPointer(directoryIndex(key));
        return getFromBucket(bucketOffset, key);
    }

    public boolean remove(int key) {
        ensureOpen();

        int directoryIndex = directoryIndex(key);
        long bucketOffset = readDirectoryPointer(directoryIndex);
        boolean removed = removeFromBucket(bucketOffset, key);

        if (!removed) {
            return false;
        }

        tryMerge(directoryIndex);
        return true;
    }

    public int uniqueBucketCountForTest() {
        ensureOpen();

        Set<Long> unique = new HashSet<>(Math.max(16, directoryLength() * 2));
        for (int i = 0; i < directoryLength(); i++) {
            unique.add(readDirectoryPointer(i));
        }
        return unique.size();
    }

    public Path filePath() {
        return filePath;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        IOException error = null;

        try {
            if (buffer != null) {
                buffer.force();
            }
        } catch (Exception e) {
            error = new IOException("Failed to flush mapped buffer", e);
        }

        try {
            channel.close();
        } catch (IOException e) {
            if (error == null) {
                error = e;
            }
        }

        if (deleteOnClose) {
            try {
                Files.deleteIfExists(filePath);
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                }
            }
        }

        if (error != null) {
            throw new UncheckedIOException("Failed to close extendable hash table", error);
        }
    }

    // ---- Алгоритм расширяемого хеширования ----
    private int directoryIndex(int key) {
        if (globalDepth == 0) {
            return 0;
        }

        int mask = (1 << globalDepth) - 1;
        return hash(key) & mask;
    }

    private void splitBucket(int splitDirectoryIndex) {
        long oldBucketOffset = readDirectoryPointer(splitDirectoryIndex);
        int oldDepth = bucketLocalDepth(oldBucketOffset);

        if (oldDepth == globalDepth) {
            doubleDirectory();
        }

        int splitBit = 1 << oldDepth;

        long newBucketOffset = allocateBucket(oldDepth + 1);
        writeBucketLocalDepth(oldBucketOffset, oldDepth + 1);

        for (int i = 0; i < directoryLength(); i++) {
            if (readDirectoryPointer(i) == oldBucketOffset && (i & splitBit) != 0) {
                writeDirectoryPointer(i, newBucketOffset);
            }
        }

        int count = bucketSize(oldBucketOffset);
        int[] keys = new int[count];
        int[] values = new int[count];

        for (int i = 0; i < count; i++) {
            keys[i] = readBucketKey(oldBucketOffset, i);
            values[i] = readBucketValue(oldBucketOffset, i);
        }

        writeBucketSize(oldBucketOffset, 0);

        for (int i = 0; i < count; i++) {
            long targetOffset = ((hash(keys[i]) & splitBit) == 0) ? oldBucketOffset : newBucketOffset;
            PutResult result = putIntoBucket(targetOffset, keys[i], values[i]);
            if (result != PutResult.INSERTED) {
                throw new IllegalStateException("Повторная вставка после split не удалась");
            }
        }
    }

    private void tryMerge(int directoryIndex) {
        while (true) {
            long bucketOffset = readDirectoryPointer(directoryIndex);
            int localDepth = bucketLocalDepth(bucketOffset);

            if (localDepth == 0) {
                return;
            }

            int buddyIndex = directoryIndex ^ (1 << (localDepth - 1));
            long buddyOffset = readDirectoryPointer(buddyIndex);

            if (buddyOffset == bucketOffset) {
                return;
            }

            if (bucketLocalDepth(buddyOffset) != localDepth) {
                return;
            }

            int totalSize = bucketSize(bucketOffset) + bucketSize(buddyOffset);
            if (totalSize > bucketCapacity) {
                return;
            }

            long survivorOffset = bucketOffset;
            long victimOffset = buddyOffset;

            if (bucketSize(victimOffset) > bucketSize(survivorOffset)) {
                long tmp = survivorOffset;
                survivorOffset = victimOffset;
                victimOffset = tmp;
            }

            int survivorSize = bucketSize(survivorOffset);
            int victimSize = bucketSize(victimOffset);

            for (int i = 0; i < victimSize; i++) {
                writeBucketKey(survivorOffset, survivorSize + i, readBucketKey(victimOffset, i));
                writeBucketValue(survivorOffset, survivorSize + i, readBucketValue(victimOffset, i));
            }

            writeBucketSize(survivorOffset, survivorSize + victimSize);
            writeBucketLocalDepth(survivorOffset, localDepth - 1);

            int newDepth = localDepth - 1;
            int base = (newDepth == 0) ? 0 : (directoryIndex & ((1 << newDepth) - 1));
            int step = (newDepth == 0) ? 1 : (1 << newDepth);

            for (int i = base; i < directoryLength(); i += step) {
                writeDirectoryPointer(i, survivorOffset);
            }
        }
    }

    private void doubleDirectory() {
        if (globalDepth == MAX_GLOBAL_DEPTH) {
            throw new IllegalStateException("Достигнут максимальный globalDepth: " + MAX_GLOBAL_DEPTH);
        }

        int oldLength = directoryLength();
        for (int i = 0; i < oldLength; i++) {
            writeDirectoryPointer(i + oldLength, readDirectoryPointer(i));
        }

        globalDepth++;
        writeHeaderInt(OFF_GLOBAL_DEPTH, globalDepth);
    }

    private int hash(int key) {
        long x = Integer.toUnsignedLong(key);
        long aa = Integer.toUnsignedLong(hashA);
        long bb = Integer.toUnsignedLong(hashB);
        int h = (int) (aa * x + bb);

        // fmix32 из MurmurHash3
        h ^= (h >>> 16);
        h *= 0x85ebca6b;
        h ^= (h >>> 13);
        h *= 0xc2b2ae35;
        h ^= (h >>> 16);

        return h;
    }

    // ---- Операции с бакетами ----
    private PutResult putIntoBucket(long bucketOffset, int key, int value) {
        int position = indexOf(bucketOffset, key);
        if (position >= 0) {
            writeBucketValue(bucketOffset, position, value);
            return PutResult.UPDATED;
        }

        int size = bucketSize(bucketOffset);
        if (size == bucketCapacity) {
            return PutResult.FULL;
        }

        writeBucketKey(bucketOffset, size, key);
        writeBucketValue(bucketOffset, size, value);
        writeBucketSize(bucketOffset, size + 1);
        return PutResult.INSERTED;
    }

    private Integer getFromBucket(long bucketOffset, int key) {
        int position = indexOf(bucketOffset, key);
        return (position == NOT_FOUND) ? null : readBucketValue(bucketOffset, position);
    }

    private boolean removeFromBucket(long bucketOffset, int key) {
        int position = indexOf(bucketOffset, key);
        if (position == NOT_FOUND) {
            return false;
        }

        int size = bucketSize(bucketOffset);
        int last = size - 1;

        writeBucketKey(bucketOffset, position, readBucketKey(bucketOffset, last));
        writeBucketValue(bucketOffset, position, readBucketValue(bucketOffset, last));
        writeBucketSize(bucketOffset, last);

        return true;
    }

    private int indexOf(long bucketOffset, int key) {
        int size = bucketSize(bucketOffset);
        for (int i = 0; i < size; i++) {
            if (readBucketKey(bucketOffset, i) == key) {
                return i;
            }
        }
        return NOT_FOUND;
    }

    // ---- Инициализация и валидация ----
    private void initializeNewTable(long seed) {
        SplittableRandom random = new SplittableRandom(seed);
        this.hashA = random.nextInt() | 1;
        this.hashB = random.nextInt();
        this.globalDepth = 0;
        this.nextFreeOffset = BUCKET_REGION_OFFSET;

        writeHeader();

        long initialBucketOffset = allocateBucket(0);
        writeDirectoryPointer(0, initialBucketOffset);
    }

    private void validateHeader() {
        if (readHeaderInt(OFF_MAGIC) != MAGIC) {
            throw new IllegalStateException("Файл таблицы поврежден: неверный magic");
        }
        if (readHeaderInt(OFF_VERSION) != VERSION) {
            throw new IllegalStateException("Неподдерживаемая версия таблицы");
        }
        if (readHeaderInt(OFF_MAX_GLOBAL_DEPTH) != MAX_GLOBAL_DEPTH) {
            throw new IllegalStateException("Неподдерживаемый формат структуры файла таблицы");
        }
    }

    private void writeHeader() {
        writeHeaderInt(OFF_MAGIC, MAGIC);
        writeHeaderInt(OFF_VERSION, VERSION);
        writeHeaderInt(OFF_BUCKET_CAPACITY, bucketCapacity);
        writeHeaderInt(OFF_GLOBAL_DEPTH, globalDepth);
        writeHeaderInt(OFF_HASH_A, hashA);
        writeHeaderInt(OFF_HASH_B, hashB);
        writeHeaderLong(OFF_NEXT_FREE, nextFreeOffset);
        writeHeaderInt(OFF_MAX_GLOBAL_DEPTH, MAX_GLOBAL_DEPTH);
    }

    private static void validateBucketCapacity(int bucketCapacity) {
        if (bucketCapacity <= 0) {
            throw new IllegalArgumentException("bucketCapacity должен быть > 0");
        }
    }

    private static int bucketRecordSizeFor(int bucketCapacity) {
        return BUCKET_HEADER_SIZE + (bucketCapacity * Integer.BYTES * 2);
    }

    private int directoryLength() {
        return 1 << globalDepth;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Хеш-таблица уже закрыта");
        }
    }

    private static Path createTempFile() {
        try {
            Path path = Files.createTempFile("extendable-hash-", ".dat");
            path.toFile().deleteOnExit();
            return path;
        } catch (IOException e) {
            throw new UncheckedIOException("Не удалось создать временный файл для хеш-таблицы", e);
        }
    }

    // ---- Рост файла и выделение памяти ----
    private void ensureMappedSize(long minSize) {
        if (minSize <= mappedSize) {
            return;
        }

        long newSize = Math.max(minSize, mappedSize == 0 ? MIN_FILE_SIZE : mappedSize * 2);
        if (newSize > Integer.MAX_VALUE) {
            throw new IllegalStateException("Файл таблицы вырос больше лимита mmap ByteBuffer");
        }

        try {
            long currentSize = channel.size();
            if (currentSize < newSize) {
                channel.position(newSize - 1);
                channel.write(ByteBuffer.wrap(new byte[]{0}));
            }
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);
            mappedSize = newSize;
        } catch (IOException e) {
            throw new UncheckedIOException("Не удалось перемапить файл таблицы", e);
        }
    }

    private long allocateBucket(int localDepth) {
        long bucketOffset = nextFreeOffset;
        long updatedNextFree = bucketOffset + bucketRecordSize;

        ensureMappedSize(updatedNextFree);

        nextFreeOffset = updatedNextFree;
        writeHeaderLong(OFF_NEXT_FREE, nextFreeOffset);

        writeBucketLocalDepth(bucketOffset, localDepth);
        writeBucketSize(bucketOffset, 0);

        for (int i = 0; i < bucketCapacity; i++) {
            writeBucketKey(bucketOffset, i, 0);
            writeBucketValue(bucketOffset, i, 0);
        }

        return bucketOffset;
    }

    // ---- Хелперы директории ----
    private long directoryOffset(int directoryIndex) {
        return DIRECTORY_OFFSET + ((long) directoryIndex * Long.BYTES);
    }

    private long readDirectoryPointer(int directoryIndex) {
        return readLongAt(directoryOffset(directoryIndex));
    }

    private void writeDirectoryPointer(int directoryIndex, long bucketOffset) {
        writeLongAt(directoryOffset(directoryIndex), bucketOffset);
    }

    // ---- Хелперы структуры бакета ----
    private int bucketLocalDepth(long bucketOffset) {
        return readIntAt(bucketOffset + BUCKET_OFF_LOCAL_DEPTH);
    }

    private void writeBucketLocalDepth(long bucketOffset, int localDepth) {
        writeIntAt(bucketOffset + BUCKET_OFF_LOCAL_DEPTH, localDepth);
    }

    private int bucketSize(long bucketOffset) {
        return readIntAt(bucketOffset + BUCKET_OFF_SIZE);
    }

    private void writeBucketSize(long bucketOffset, int size) {
        writeIntAt(bucketOffset + BUCKET_OFF_SIZE, size);
    }

    private long bucketKeysOffset(long bucketOffset) {
        return bucketOffset + BUCKET_HEADER_SIZE;
    }

    private long bucketValuesOffset(long bucketOffset) {
        return bucketKeysOffset(bucketOffset) + ((long) bucketCapacity * Integer.BYTES);
    }

    private int readBucketKey(long bucketOffset, int slot) {
        long offset = bucketKeysOffset(bucketOffset) + ((long) slot * Integer.BYTES);
        return readIntAt(offset);
    }

    private void writeBucketKey(long bucketOffset, int slot, int key) {
        long offset = bucketKeysOffset(bucketOffset) + ((long) slot * Integer.BYTES);
        writeIntAt(offset, key);
    }

    private int readBucketValue(long bucketOffset, int slot) {
        long offset = bucketValuesOffset(bucketOffset) + ((long) slot * Integer.BYTES);
        return readIntAt(offset);
    }

    private void writeBucketValue(long bucketOffset, int slot, int value) {
        long offset = bucketValuesOffset(bucketOffset) + ((long) slot * Integer.BYTES);
        writeIntAt(offset, value);
    }

    // ---- Хелперы заголовка ----
    private int readHeaderInt(int offset) {
        return buffer.getInt(offset);
    }

    private void writeHeaderInt(int offset, int value) {
        buffer.putInt(offset, value);
    }

    private long readHeaderLong(int offset) {
        return buffer.getLong(offset);
    }

    private void writeHeaderLong(int offset, long value) {
        buffer.putLong(offset, value);
    }

    // ---- Низкоуровневые примитивы mmap ----
    private int readIntAt(long offset) {
        return buffer.getInt(toBufferIndex(offset, Integer.BYTES));
    }

    private void writeIntAt(long offset, int value) {
        buffer.putInt(toBufferIndex(offset, Integer.BYTES), value);
    }

    private long readLongAt(long offset) {
        return buffer.getLong(toBufferIndex(offset, Long.BYTES));
    }

    private void writeLongAt(long offset, long value) {
        buffer.putLong(toBufferIndex(offset, Long.BYTES), value);
    }

    private int toBufferIndex(long offset, int length) {
        long limit = offset + length;
        if (offset < 0 || limit < offset || limit > mappedSize) {
            throw new IllegalStateException("Файл таблицы поврежден: некорректные смещения");
        }
        return (int) offset;
    }
}
