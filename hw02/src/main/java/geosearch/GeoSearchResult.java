package geosearch;

import java.util.Objects;

public record GeoSearchResult<T>(GeoObject<T> object, double distanceMeters) {

    public GeoSearchResult {
        Objects.requireNonNull(object, "object must not be null");
        if (!Double.isFinite(distanceMeters) || distanceMeters < 0.0) {
            throw new IllegalArgumentException("distanceMeters must be finite and >= 0");
        }
    }
}
