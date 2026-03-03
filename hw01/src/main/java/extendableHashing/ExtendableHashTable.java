package extendableHashing;


import java.util.*;

public class ExtendableHashTable {
    private final int bucketCapacity;
    private int globalDepth;
    private Bucket[] directory;
    private final int a;
    private final int b;

    public ExtendableHashTable(int bucketCapacity) {
        this.bucketCapacity = bucketCapacity;
        this.globalDepth = 0;
        this.directory = new Bucket[]{new Bucket(0, bucketCapacity)};

        SplittableRandom random = new SplittableRandom();
        this.a = random.nextInt() | 1; // нечетное
        this.b = random.nextInt();
    }

    // нужен для теста merge
    public ExtendableHashTable(int bucketCapacity, long seed) {
        this.bucketCapacity = bucketCapacity;
        this.globalDepth = 0;
        this.directory = new Bucket[]{new Bucket(0, bucketCapacity)};

        SplittableRandom random = new SplittableRandom(seed);
        this.a = random.nextInt() | 1;
        this.b = random.nextInt();
    }


    private int directoryIndex(int key) {
        if (globalDepth == 0) {
            return 0;
        }
        int mask = (1 << globalDepth) - 1;
        return hash(key) & mask; // обнуление всех битов кроме младших
    }

    public void put(int key, int value) {
        while (true) {
            int idx = directoryIndex(key);
            Bucket bucket = directory[idx];

            PutResult result = bucket.put(key, value);
            if (result == PutResult.INSERTED || result == PutResult.UPDATED) {
                return;
            }
            splitBucket(idx);
        }
    }

    public Integer get(int key) {
        Bucket bucket = directory[directoryIndex(key)];
        return bucket.get(key);
    }

    public boolean remove(int key) {
        int idx = directoryIndex(key);
        Bucket bucket = directory[idx];
        boolean removed = bucket.remove(key);

        if (!removed) {
            return false;
        }

        tryMerge(idx);
        return true;
    }

    private void tryMerge(int idx) {
        while (true) {
            Bucket bucket = directory[idx];
            int d = bucket.localDepth;

            if (d == 0) { // если localDepth == 0, бакет самый верхний, у него нет buddy уровня -1, слить дальше невозможно
                return;
            }

            int buddyIndex = idx ^ (1 << (d - 1)); // buddy отличается ровно одним битом, тем, по которому бакет делили в последний раз
            Bucket buddy = directory[buddyIndex];

            if (buddy == bucket) {
                return; // указатели уже на один бакет
            }

            if (buddy.localDepth != d) { // Buddy должен быть на том же уровне
                return;
            }
            int total = bucket.size + buddy.size;
            if (total > bucketCapacity) { // если в один бакет не помещается — merge нельзя
                return;
            }

            Bucket survivor = bucket;
            Bucket victim = buddy; // выживет текущий бакет, а buddy будет жертвой

            if (victim.size > survivor.size) { // копируем меньший бакет в больший
                survivor = buddy;
                victim = bucket;
            }

            System.arraycopy(victim.keys, 0, survivor.keys, survivor.size, victim.size);
            System.arraycopy(victim.values, 0, survivor.values, survivor.size, victim.size);
            survivor.size += victim.size;

            survivor.localDepth = d - 1;

            int newDepth = d - 1; // после объединения уровень разделения уменьшается

            int base;
            int step;

            if (newDepth == 0) {
                base = 0;
                step = 1;
            } else {
                int mask = (1 << newDepth) - 1;
                base = idx & mask; // base это шаблон младших newDepth бит, который должен обслуживать survivor
                step = 1 << newDepth;
            }

            for (int i = base; i < directory.length; i += step) {
                directory[i] = survivor;
            }
        }
    }

    private void doubleDirectory() {
        Bucket[] newDirectory = new Bucket[directory.length * 2];
        for (int i = 0; i < directory.length; i++) {
            newDirectory[i] = directory[i]; // копируем элемент в первую половину нового каталога
            newDirectory[i + directory.length] = directory[i]; // копируем элемент в первую половину нового каталога
        }
        directory = newDirectory;
        globalDepth++;
    }

    private void splitBucket(int dirIndex) {
        Bucket oldBucket = directory[dirIndex];

        if (oldBucket.localDepth == globalDepth) {
            doubleDirectory();
        }

        int oldDepth = oldBucket.localDepth;
        int splitBit = 1 << oldDepth;

        Bucket newBucket = new Bucket(oldDepth + 1, bucketCapacity);
        oldBucket.localDepth = oldDepth + 1;

        for (int i = 0; i < directory.length; i++) {
            if (directory[i] == oldBucket && (i & splitBit) != 0) {
                directory[i] = newBucket;
            }
        }

        int count = oldBucket.size;
        int[] tmpKeys = new int[count];
        int[] tmpValues = new int[count];
        for (int i = 0; i < count; i++) {
            tmpKeys[i] = oldBucket.keys[i];
            tmpValues[i] = oldBucket.values[i];
        }

        oldBucket.size = 0;

        for (int i = 0; i < count; i++) {
            int k = tmpKeys[i];
            int v = tmpValues[i];
            int h = hash(k);

            Bucket target = ((h & splitBit) == 0) ? oldBucket : newBucket;
            PutResult result = target.put(k, v);
            if (result != PutResult.INSERTED) {
                throw new IllegalStateException("Reinsert failed");
            }
        }
    }

    private int hash(int key) {
        long x = Integer.toUnsignedLong(key);
        long aa = Integer.toUnsignedLong(a);
        long bb = Integer.toUnsignedLong(b);
        int h = (int) (aa * x + bb);

        // ниже копипаст логики fmix32 из MurmurHash3
        h ^= (h >>> 16);
        h *= 0x85ebca6b;
        h ^= (h >>> 13);
        h *= 0xc2b2ae35;
        h ^= (h >>> 16);

        return h;
    }

    public int uniqueBucketCountForTest() {
        // выясняем сколько реальных бакетов существует
        Set<Bucket> uniq = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Bucket b : directory) {
            uniq.add(b);
        }
        return uniq.size();
    }
}
