package extendableHashing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static extendableHashing.ExtendableHashFileFormat.*;

public class ExtendableHashTable implements AutoCloseable {
    private static final int NOT_FOUND = -1;
    private final int bucketCapacity;
    private final int bucketRecordSize;
    private final Path storagePath;
    private final FileChannel channel;
    private MappedByteBuffer buffer;
    private long mappedSize;
    private int globalDepth;
    private long nextFreeOffset;
    private boolean closed;

    /**
     * Создаёт таблицу и подготавливает файл хранения
     */
    public ExtendableHashTable(int bucketCapacity) {
        this.bucketCapacity = bucketCapacity;
        this.bucketRecordSize = bucketRecordSizeFor(bucketCapacity);
        this.storagePath = createTempFile();
        this.channel = openChannel(
                storagePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        this.mappedSize = 0;
        ensureMappedSize(Math.max(MIN_FILE_SIZE, BUCKET_REGION_OFFSET + bucketRecordSize));

        this.globalDepth = 0;
        this.nextFreeOffset = BUCKET_REGION_OFFSET;

        writeHeaderInt(OFF_BUCKET_CAPACITY, this.bucketCapacity);
        writeHeaderInt(OFF_GLOBAL_DEPTH, this.globalDepth);
        writeHeaderLong(OFF_NEXT_FREE, this.nextFreeOffset);

        long initialBucketOffset = allocateBucket(0);
        writeDirectoryPointer(0, initialBucketOffset);
    }

    /**
     * Добавляет новую пару ключ-значение или обновляет
     */
    public void put(int key, int value) {
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

    /**
     * Возвращает значение по ключу
     */
    public Integer get(int key) {
        long bucketOffset = readDirectoryPointer(directoryIndex(key));
        int position = indexOf(bucketOffset, key);
        return (position == NOT_FOUND) ? null : readBucketValue(bucketOffset, position);
    }

    /**
     * Удаляет ключ
     */
    public boolean remove(int key) {
        int directoryIndex = directoryIndex(key);
        long bucketOffset = readDirectoryPointer(directoryIndex);
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

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        if (buffer != null) {
            buffer.force();
        }

        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        try {
            Files.deleteIfExists(storagePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Вычисляет индекс директории по ключу
     */
    private int directoryIndex(int key) {
        if (globalDepth == 0) {
            return 0;
        }

        int mask = (1 << globalDepth) - 1;
        return hash(key) & mask;
    }

    /**
     * Делит переполненный бакет на два
     */
    private void splitBucket(int splitDirectoryIndex) {
        long oldBucketOffset = readDirectoryPointer(splitDirectoryIndex);
        int oldDepth = bucketLocalDepth(oldBucketOffset);

        if (oldDepth == globalDepth) {
            doubleDirectory();
        }

        int splitBit = 1 << oldDepth;

        long newBucketOffset = allocateBucket(oldDepth + 1);
        writeBucketLocalDepth(oldBucketOffset, oldDepth + 1);

        int directoryLen = directoryLength();
        int baseIndex = splitDirectoryIndex & (splitBit - 1);
        int start = baseIndex | splitBit;
        int step = splitBit << 1;
        for (int i = start; i < directoryLen; i += step) {
            writeDirectoryPointer(i, newBucketOffset);
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
                throw new IllegalStateException("split reinsertion failed");
            }
        }
    }

    /**
     * Пытается объединять совместимые бакеты после удаления записи
     */
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

    /**
     * Удваивает директории
     */
    private void doubleDirectory() {
        if (globalDepth == MAX_GLOBAL_DEPTH) {
            throw new IllegalStateException("max globalDepth reached");
        }

        int oldLength = directoryLength();
        for (int i = 0; i < oldLength; i++) {
            writeDirectoryPointer(i + oldLength, readDirectoryPointer(i));
        }

        globalDepth++;
        writeHeaderInt(OFF_GLOBAL_DEPTH, globalDepth);
    }

    /**
     * Вычисляет хэш для ключа
     */
    private int hash(int key) {
        int h = Integer.hashCode(key);
        return h ^ (h >>> 16);
    }

    /**
     * Вставляет или обновляет запись внутри одного бакета
     */
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

    /**
     * Ищет позицию ключа внутри бакета
     */
    private int indexOf(long bucketOffset, int key) {
        int size = bucketSize(bucketOffset);
        for (int i = 0; i < size; i++) {
            if (readBucketKey(bucketOffset, i) == key) {
                return i;
            }
        }
        return NOT_FOUND;
    }

    /**
     * Возвращает длину директории по глобальной глубине
     */
    private int directoryLength() {
        return 1 << globalDepth;
    }

    /**
     * Создаёт временный файл
     */
    private static Path createTempFile() {
        try {
            return Files.createTempFile("extendable-hash-", ".dat");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Увеличивает размер файла до нужного размера
     */
    private void ensureMappedSize(long minSize) {
        if (minSize <= mappedSize) {
            return;
        }

        long newSize = Math.max(minSize, mappedSize == 0 ? MIN_FILE_SIZE : mappedSize * 2);
        if (newSize > Integer.MAX_VALUE) {
            throw new IllegalStateException("file is too large for mmap ByteBuffer");
        }

        long currentSize;
        try {
            currentSize = channel.size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (currentSize < newSize) {
            try {
                channel.position(newSize - 1);
                channel.write(ByteBuffer.wrap(new byte[]{0}));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        try {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        mappedSize = newSize;
    }

    /**
     * Выделяет место под новый бакет
     */
    private long allocateBucket(int localDepth) {
        long bucketOffset = nextFreeOffset;
        long updatedNextFree = bucketOffset + bucketRecordSize;

        ensureMappedSize(updatedNextFree);

        nextFreeOffset = updatedNextFree;
        writeHeaderLong(OFF_NEXT_FREE, nextFreeOffset);

        writeBucketLocalDepth(bucketOffset, localDepth);
        writeBucketSize(bucketOffset, 0);
        return bucketOffset;
    }

    /**
     * Читает указатель на бакет из заданной ячейки директории
     */
    private long readDirectoryPointer(int directoryIndex) {
        long offset = directoryOffset(directoryIndex);
        return buffer.getLong(toBufferIndex(offset));
    }

    /**
     * Записывает в ячейку директории смещение бакета
     */
    private void writeDirectoryPointer(int directoryIndex, long bucketOffset) {
        long offset = directoryOffset(directoryIndex);
        buffer.putLong(toBufferIndex(offset), bucketOffset);
    }

    /**
     * Читает локальную глубину бакета
     */
    private int bucketLocalDepth(long bucketOffset) {
        return readIntAt(bucketOffset + BUCKET_OFF_LOCAL_DEPTH);
    }

    /**
     * Записывает локальную глубину бакета
     */
    private void writeBucketLocalDepth(long bucketOffset, int localDepth) {
        writeIntAt(bucketOffset + BUCKET_OFF_LOCAL_DEPTH, localDepth);
    }

    /**
     * Читает текущее количество записей в бакете
     */
    private int bucketSize(long bucketOffset) {
        return readIntAt(bucketOffset + BUCKET_OFF_SIZE);
    }

    /**
     * Записывает количество занятых позиций в бакете
     */
    private void writeBucketSize(long bucketOffset, int size) {
        writeIntAt(bucketOffset + BUCKET_OFF_SIZE, size);
    }

    /**
     * Читает ключ из бакета
     */
    private int readBucketKey(long bucketOffset, int slot) {
        long offset = bucketKeysOffset(bucketOffset) + ((long) slot * Integer.BYTES);
        return readIntAt(offset);
    }

    /**
     * Записывает ключ в бакет
     */
    private void writeBucketKey(long bucketOffset, int slot, int key) {
        long offset = bucketKeysOffset(bucketOffset) + ((long) slot * Integer.BYTES);
        writeIntAt(offset, key);
    }

    /**
     * Читает значение из бакета
     */
    private int readBucketValue(long bucketOffset, int slot) {
        long offset = bucketValuesOffset(bucketOffset, bucketCapacity) + ((long) slot * Integer.BYTES);
        return readIntAt(offset);
    }

    /**
     * Записывает значение в бакет
     */
    private void writeBucketValue(long bucketOffset, int slot, int value) {
        long offset = bucketValuesOffset(bucketOffset, bucketCapacity) + ((long) slot * Integer.BYTES);
        writeIntAt(offset, value);
    }

    /**
     * Записывает целое число в заголовок файла
     */
    private void writeHeaderInt(int offset, int value) {
        buffer.putInt(offset, value);
    }

    /**
     * Записывает длинное число в заголовок файла
     */
    private void writeHeaderLong(int offset, long value) {
        buffer.putLong(offset, value);
    }

    /**
     * Читает целое число в файле
     */
    private int readIntAt(long offset) {
        return buffer.getInt(toBufferIndex(offset));
    }

    /**
     * Записывает целое число в файл
     */
    private void writeIntAt(long offset, int value) {
        buffer.putInt(toBufferIndex(offset), value);
    }

    /**
     * Преобразует длинное смещение файла в индекс буфера
     */
    private int toBufferIndex(long offset) {
        return (int) offset;
    }

    /**
     * Открывает файловый канал
     */
    private static FileChannel openChannel(Path filePath, StandardOpenOption... options) {
        try {
            return FileChannel.open(filePath, options);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
