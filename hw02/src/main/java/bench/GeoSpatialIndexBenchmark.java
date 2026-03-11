package bench;

import geosearch.GeoObject;
import geosearch.GeoSearchResult;
import geosearch.GeoSpatialIndex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class GeoSpatialIndexBenchmark {
    private static final double EARTH_RADIUS_METERS = 6_371_008.8;
    private static final int QUERY_COUNT = 4_096;

    @State(Scope.Benchmark)
    public static class BuildState {
        @Param({"10000", "100000", "300000"})
        public int n;

        @Param({"120.0", "500.0"})
        public double cellSizeMeters;

        @Param({"42"})
        public long seed;

        GeneratedData data;

        @Setup(Level.Trial)
        public void setup() {
            data = GeneratedData.generate(n, QUERY_COUNT, seed);
        }
    }

    @State(Scope.Thread)
    public static class QueryState {
        @Param({"10000", "100000", "300000"})
        public int n;

        @Param({"120.0", "500.0"})
        public double cellSizeMeters;

        @Param({"100.0", "1000.0", "10000.0", "50000.0"})
        public double radiusMeters;

        @Param({"42"})
        public long seed;

        GeneratedData data;
        GeoSpatialIndex<Integer> geoIndex;
        NaiveGeoIndex<Integer> naiveIndex;
        int queryPos;

        @Setup(Level.Trial)
        public void setup() {
            data = GeneratedData.generate(n, QUERY_COUNT, seed);
            geoIndex = buildGeoIndex(data, cellSizeMeters);
            naiveIndex = buildNaiveIndex(data);
            verifyQueryParity(geoIndex, naiveIndex, data, radiusMeters);
            queryPos = 0;
        }

        int nextQueryIndex() {
            int idx = queryPos;
            queryPos++;
            if (queryPos == data.queryLatitudes.length) {
                queryPos = 0;
            }
            return idx;
        }
    }

    @State(Scope.Thread)
    public static class MutationState {
        @Param({"10000", "100000"})
        public int n;

        @Param({"120.0", "500.0"})
        public double cellSizeMeters;

        @Param({"42"})
        public long seed;

        GeneratedData data;
        GeoSpatialIndex<Integer> geoIndex;
        NaiveGeoIndex<Integer> naiveIndex;
        int pos;

        @Setup(Level.Iteration)
        public void setup() {
            data = GeneratedData.generate(n, QUERY_COUNT, seed);
            geoIndex = buildGeoIndex(data, cellSizeMeters);
            naiveIndex = buildNaiveIndex(data);
            pos = 0;
        }

        int nextPos() {
            int idx = pos;
            pos++;
            if (pos == data.ids.length) {
                pos = 0;
            }
            return idx;
        }
    }

    @Benchmark
    public int buildGeoSpatialIndex(BuildState state, Blackhole bh) {
        GeoSpatialIndex<Integer> index = buildGeoIndex(state.data, state.cellSizeMeters);
        bh.consume(index.getById(state.data.ids[state.data.ids.length / 2]));
        return index.size();
    }

    @Benchmark
    public int buildNaiveIndex(BuildState state, Blackhole bh) {
        NaiveGeoIndex<Integer> index = buildNaiveIndex(state.data);
        bh.consume(index.getById(state.data.ids[state.data.ids.length / 2]));
        return index.size();
    }

    @Benchmark
    public int nearbyQueryGeoSpatialIndex(QueryState state, Blackhole bh) {
        int q = state.nextQueryIndex();
        List<GeoSearchResult<Integer>> results = state.geoIndex.findNearby(
                state.data.queryLatitudes[q], state.data.queryLongitudes[q], state.radiusMeters);
        bh.consume(firstId(results));
        return results.size();
    }

    @Benchmark
    public int nearbyQueryNaive(QueryState state, Blackhole bh) {
        int q = state.nextQueryIndex();
        List<GeoSearchResult<Integer>> results = state.naiveIndex.findNearby(
                state.data.queryLatitudes[q], state.data.queryLongitudes[q], state.radiusMeters);
        bh.consume(firstId(results));
        return results.size();
    }

    @Benchmark
    public void upsertExistingGeoSpatialIndex(MutationState state) {
        int i = state.nextPos();
        state.geoIndex.put(state.data.ids[i], state.data.updateLatitudes[i], state.data.updateLongitudes[i], i);
    }

    @Benchmark
    public void upsertExistingNaive(MutationState state) {
        int i = state.nextPos();
        state.naiveIndex.put(state.data.ids[i], state.data.updateLatitudes[i], state.data.updateLongitudes[i], i);
    }

    @Benchmark
    public void removeThenPutGeoSpatialIndex(MutationState state) {
        int i = state.nextPos();
        String id = state.data.ids[i];
        state.geoIndex.remove(id);
        state.geoIndex.put(id, state.data.updateLatitudes[i], state.data.updateLongitudes[i], i);
    }

    @Benchmark
    public void removeThenPutNaive(MutationState state) {
        int i = state.nextPos();
        String id = state.data.ids[i];
        state.naiveIndex.remove(id);
        state.naiveIndex.put(id, state.data.updateLatitudes[i], state.data.updateLongitudes[i], i);
    }

    private static GeoSpatialIndex<Integer> buildGeoIndex(GeneratedData data, double cellSizeMeters) {
        GeoSpatialIndex<Integer> index = new GeoSpatialIndex<>(cellSizeMeters);
        for (int i = 0; i < data.ids.length; i++) {
            index.put(data.ids[i], data.latitudes[i], data.longitudes[i], data.payloads[i]);
        }
        return index;
    }

    private static NaiveGeoIndex<Integer> buildNaiveIndex(GeneratedData data) {
        NaiveGeoIndex<Integer> index = new NaiveGeoIndex<>();
        for (int i = 0; i < data.ids.length; i++) {
            index.put(data.ids[i], data.latitudes[i], data.longitudes[i], data.payloads[i]);
        }
        return index;
    }

    private static void verifyQueryParity(
            GeoSpatialIndex<Integer> geoIndex,
            NaiveGeoIndex<Integer> naiveIndex,
            GeneratedData data,
            double radiusMeters) {

        int checks = Math.min(32, data.queryLatitudes.length);
        for (int i = 0; i < checks; i++) {
            List<GeoSearchResult<Integer>> geo = geoIndex.findNearby(
                    data.queryLatitudes[i], data.queryLongitudes[i], radiusMeters);
            List<GeoSearchResult<Integer>> naive = naiveIndex.findNearby(
                    data.queryLatitudes[i], data.queryLongitudes[i], radiusMeters);

            if (geo.size() != naive.size()) {
                throw new IllegalStateException("parity check failed: result size mismatch");
            }
            for (int j = 0; j < geo.size(); j++) {
                if (!geo.get(j).object().id().equals(naive.get(j).object().id())) {
                    throw new IllegalStateException("parity check failed: object order mismatch");
                }
            }
        }
    }

    private static String firstId(List<GeoSearchResult<Integer>> results) {
        if (results.isEmpty()) {
            return null;
        }
        return results.get(0).object().id();
    }

    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);
        double dLat = lat2Rad - lat1Rad;
        double dLon = Math.toRadians(lon2 - lon1);

        double sinHalfLat = Math.sin(dLat / 2.0);
        double sinHalfLon = Math.sin(dLon / 2.0);

        double a = sinHalfLat * sinHalfLat
                + Math.cos(lat1Rad) * Math.cos(lat2Rad) * sinHalfLon * sinHalfLon;
        double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));
        return EARTH_RADIUS_METERS * c;
    }

    private static double randomLatitude(SplittableRandom rnd) {
        int mode = rnd.nextInt(12);
        if (mode == 0) {
            return -90.0 + rnd.nextDouble(0.0, 0.2);
        }
        if (mode == 1) {
            return 90.0 - rnd.nextDouble(0.0, 0.2);
        }
        return rnd.nextDouble(-90.0, 90.0);
    }

    private static double randomLongitude(SplittableRandom rnd) {
        int mode = rnd.nextInt(14);
        if (mode == 0) {
            return -180.0 + rnd.nextDouble(0.0, 0.2);
        }
        if (mode == 1) {
            return 180.0 - rnd.nextDouble(0.0, 0.2);
        }
        if (mode == 2) {
            return rnd.nextDouble(-180.0, -179.7);
        }
        if (mode == 3) {
            return rnd.nextDouble(179.7, 180.0);
        }
        return rnd.nextDouble(-180.0, 180.0);
    }

    private static double normalizeLongitude(double longitude) {
        double result = longitude;
        while (result > 180.0) {
            result -= 360.0;
        }
        while (result < -180.0) {
            result += 360.0;
        }
        return result;
    }

    private static double clampLatitude(double latitude) {
        return Math.max(-90.0, Math.min(90.0, latitude));
    }

    private static final class GeneratedData {
        private final String[] ids;
        private final double[] latitudes;
        private final double[] longitudes;
        private final int[] payloads;
        private final double[] queryLatitudes;
        private final double[] queryLongitudes;
        private final double[] updateLatitudes;
        private final double[] updateLongitudes;

        private GeneratedData(
                String[] ids,
                double[] latitudes,
                double[] longitudes,
                int[] payloads,
                double[] queryLatitudes,
                double[] queryLongitudes,
                double[] updateLatitudes,
                double[] updateLongitudes) {
            this.ids = ids;
            this.latitudes = latitudes;
            this.longitudes = longitudes;
            this.payloads = payloads;
            this.queryLatitudes = queryLatitudes;
            this.queryLongitudes = queryLongitudes;
            this.updateLatitudes = updateLatitudes;
            this.updateLongitudes = updateLongitudes;
        }

        private static GeneratedData generate(int n, int queryCount, long seed) {
            SplittableRandom rnd = new SplittableRandom(seed);

            String[] ids = new String[n];
            double[] latitudes = new double[n];
            double[] longitudes = new double[n];
            int[] payloads = new int[n];
            double[] updateLatitudes = new double[n];
            double[] updateLongitudes = new double[n];

            for (int i = 0; i < n; i++) {
                ids[i] = "id-" + i;
                latitudes[i] = randomLatitude(rnd);
                longitudes[i] = randomLongitude(rnd);
                payloads[i] = rnd.nextInt();

                updateLatitudes[i] = clampLatitude(latitudes[i] + rnd.nextDouble(-0.2, 0.2));
                updateLongitudes[i] = normalizeLongitude(longitudes[i] + rnd.nextDouble(-0.2, 0.2));
            }

            double[] queryLatitudes = new double[queryCount];
            double[] queryLongitudes = new double[queryCount];
            for (int i = 0; i < queryCount; i++) {
                if (i % 4 == 0) {
                    int p = rnd.nextInt(n);
                    queryLatitudes[i] = clampLatitude(latitudes[p] + rnd.nextDouble(-0.02, 0.02));
                    queryLongitudes[i] = normalizeLongitude(longitudes[p] + rnd.nextDouble(-0.02, 0.02));
                } else {
                    queryLatitudes[i] = randomLatitude(rnd);
                    queryLongitudes[i] = randomLongitude(rnd);
                }
            }

            return new GeneratedData(
                    ids, latitudes, longitudes, payloads,
                    queryLatitudes, queryLongitudes,
                    updateLatitudes, updateLongitudes);
        }
    }

    private static final class NaiveGeoIndex<T> {
        private final Map<String, GeoObject<T>> byId = new HashMap<>();

        void put(String id, double latitude, double longitude, T payload) {
            byId.put(id, new GeoObject<>(id, latitude, longitude, payload));
        }

        boolean remove(String id) {
            return byId.remove(id) != null;
        }

        GeoObject<T> getById(String id) {
            return byId.get(id);
        }

        int size() {
            return byId.size();
        }

        List<GeoSearchResult<T>> findNearby(double latitude, double longitude, double radiusMeters) {
            List<GeoSearchResult<T>> results = new ArrayList<>();
            for (GeoObject<T> object : byId.values()) {
                double distance = haversineMeters(latitude, longitude, object.latitude(), object.longitude());
                if (distance <= radiusMeters) {
                    results.add(new GeoSearchResult<>(object, distance));
                }
            }
            results.sort(Comparator
                    .comparingDouble((GeoSearchResult<T> result) -> result.distanceMeters())
                    .thenComparing(result -> result.object().id()));
            return results;
        }
    }

    public static void main(String[] args) throws Exception {
        String[] forwarded = args == null ? new String[0] : args;
        String[] withInclude = Arrays.copyOf(forwarded, forwarded.length + 1);
        withInclude[forwarded.length] = GeoSpatialIndexBenchmark.class.getName() + ".*";
        org.openjdk.jmh.Main.main(withInclude);
    }
}
