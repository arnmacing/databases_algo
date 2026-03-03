package extendableHashing;

final class Bucket {
    int localDepth; // сколько бит важно для конкретного бакета
    int size;

    final int[] keys;
    final int[] values;

    Bucket(int localDepth, int capacity) {
        this.localDepth = localDepth;
        this.keys = new int[capacity]; // капасити - кол-во пар ключ-значение
        this.values = new int[capacity];
        this.size = 0; // сколько записей сейчас реально лежит
    }

    Integer get(int key) {
        int idx = indexOf(key);
        return idx >= 0 ? values[idx] : null;
    }

    private int indexOf(int key) { // потому что размер мелкий
        for (int i = 0; i < size; i++) {
            if (keys[i] == key) {
                return i;
            }
        }
        return -1;
    }

    boolean containsKey(int key) {
        int idx = indexOf(key);
        return idx >= 0;
    }


    PutResult put(int key, int value) {
        int idx = indexOf(key);
        if (idx >= 0) {
            values[idx] = value;
            return PutResult.UPDATED;
        }
        if (size == keys.length) {
            return PutResult.FULL;
        }
        keys[size] = key;
        values[size] = value;
        size++;
        return PutResult.INSERTED;
    }

    boolean remove(int key) {
        int idx = indexOf(key);
        if (idx < 0) {
            return false;
        }
        int last = size - 1;
        keys[idx] = keys[last];
        values[idx] = values[last];
        size--;
        return true;
    }

}