package lsh;

public final class Pair {
    public final int left;
    public final int right;
    public final double similarity;

    /**
     * Создаёт объект пары документов и сохраняет их сходство
     */
    public Pair(int left, int right, double similarity) {
        this.left = left;
        this.right = right;
        this.similarity = similarity;
    }
}
