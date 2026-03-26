## GeoSpatialIndexBenchmark

### buildGeoSpatialIndex

<img src="plots/bench.GeoSpatialIndexBenchmark.buildGeoSpatialIndex__score.png" alt="score" width="72%">

#### JMH Results
**avgt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 788.476948 +- 11.836680 us/op | 743.172499 +- 2.673992 us/op |
| 100000 | 15805.132207 +- 983.466161 us/op | 16431.167851 +- 511.716338 us/op |
| 300000 | 60057.805939 +- 1153.522394 us/op | 56212.428657 +- 1543.791264 us/op |

**thrpt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 0.001240 +- 0.000005 ops/us | 0.001386 +- 0.000142 ops/us |
| 100000 | 0.000061 +- 0.000002 ops/us | 0.000066 +- 0.000002 ops/us |
| 300000 | 0.000016 +- 0.000000 ops/us | 0.000017 +- 0.000000 ops/us |

#### Async Profiler Metrics by Params/Mode
**cpu_jvm_user_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.048 | 10.052 |
| 100000 | 17.141 | 17.082 |
| 300000 | 17.509 | 17.238 |

**cpu_machine_total_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.609 | 10.555 |
| 100000 | 18.229 | 18.238 |
| 300000 | 18.447 | 18.342 |

**cpu_samples_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 101.792 | 101.708 |
| 100000 | 173.771 | 172.104 |
| 300000 | 178.271 | 176.396 |

**alloc_events_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 3677.979 | 3380.979 |
| 100000 | 1666.250 | 1783.750 |
| 300000 | 1342.354 | 1224.380 |

**alloc_events_total**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 88272 | 81144 |
| 100000 | 39990 | 42810 |
| 300000 | 32216 | 30610 |

**alloc_tlab_share_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 100.000 | 100.000 |
| 100000 | 100.000 | 100.000 |
| 300000 | 100.000 | 100.000 |

#### Notes
```text
- 
```

### buildNaiveIndex

<img src="plots/bench.GeoSpatialIndexBenchmark.buildNaiveIndex__score.png" alt="score" width="72%">

#### JMH Results
**avgt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 130.157776 +- 0.530767 us/op | 129.880918 +- 0.386195 us/op |
| 100000 | 4003.269476 +- 181.996268 us/op | 4140.655466 +- 260.561227 us/op |
| 300000 | 13125.459990 +- 596.855501 us/op | 13164.878176 +- 405.253234 us/op |

**thrpt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 0.007756 +- 0.000036 ops/us | 0.007701 +- 0.000026 ops/us |
| 100000 | 0.000257 +- 0.000010 ops/us | 0.000245 +- 0.000014 ops/us |
| 300000 | 0.000083 +- 0.000002 ops/us | 0.000083 +- 0.000002 ops/us |

#### Async Profiler Metrics by Params/Mode
**cpu_jvm_user_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.160 | 10.190 |
| 100000 | 34.302 | 33.373 |
| 300000 | 35.703 | 36.089 |

**cpu_machine_total_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.869 | 11.032 |
| 100000 | 35.656 | 34.888 |
| 300000 | 37.904 | 38.045 |

**cpu_samples_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 103.250 | 104.000 |
| 100000 | 351.146 | 341.917 |
| 300000 | 359.979 | 358.167 |

**alloc_events_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 14179.792 | 13964.104 |
| 100000 | 4380.521 | 4680.062 |
| 300000 | 3979.458 | 4115.250 |

**alloc_events_total**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 340315 | 335138 |
| 100000 | 105132 | 112322 |
| 300000 | 95507 | 98766 |

**alloc_tlab_share_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 100.000 | 100.000 |
| 100000 | 100.000 | 100.000 |
| 300000 | 100.000 | 100.000 |

#### Notes
```text
- 
```

### nearbyQueryGeoSpatialIndex

<img src="plots/bench.GeoSpatialIndexBenchmark.nearbyQueryGeoSpatialIndex__score.png" alt="score" width="72%">

#### JMH Results
**avgt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 5.556515 +- 0.015575 us/op | 5.540787 +- 0.031233 us/op |
| 100000 | 70.387985 +- 0.571321 us/op | 70.643562 +- 0.569807 us/op |
| 300000 | 245.217416 +- 4.497418 us/op | 248.427197 +- 7.226827 us/op |

**thrpt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 2.163802 +- 0.011037 ops/us | 2.135014 +- 0.013696 ops/us |
| 100000 | 0.610348 +- 0.003842 ops/us | 0.611719 +- 0.003296 ops/us |
| 300000 | 0.270099 +- 0.004164 ops/us | 0.271090 +- 0.002021 ops/us |

#### Async Profiler Metrics by Params/Mode
**cpu_jvm_user_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.000 | 10.000 |
| 100000 | 10.002 | 10.005 |
| 300000 | 10.014 | 10.013 |

**cpu_machine_total_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.513 | 10.607 |
| 100000 | 11.164 | 10.475 |
| 300000 | 10.539 | 10.481 |

**cpu_samples_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 101.818 | 102.130 |
| 100000 | 101.479 | 101.974 |
| 300000 | 101.391 | 102.057 |

**alloc_events_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 2187.490 | 2111.161 |
| 100000 | 1456.021 | 1453.911 |
| 300000 | 1301.599 | 1294.599 |

**alloc_events_total**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 52500 | 50668 |
| 100000 | 34944 | 34894 |
| 300000 | 31238 | 31070 |

**alloc_tlab_share_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 100.000 | 100.000 |
| 100000 | 100.000 | 100.000 |
| 300000 | 100.000 | 100.000 |

#### Notes
```text
- 
```

### nearbyQueryNaive

<img src="plots/bench.GeoSpatialIndexBenchmark.nearbyQueryNaive__score.png" alt="score" width="72%">

#### JMH Results
**avgt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 437.158061 +- 2.105178 us/op | 436.916784 +- 2.118130 us/op |
| 100000 | 4228.323886 +- 19.733932 us/op | 4225.977336 +- 20.100295 us/op |
| 300000 | 12425.767117 +- 49.734052 us/op | 12479.580274 +- 49.488330 us/op |

**thrpt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 0.002285 +- 0.000009 ops/us | 0.002290 +- 0.000010 ops/us |
| 100000 | 0.000236 +- 0.000001 ops/us | 0.000237 +- 0.000001 ops/us |
| 300000 | 0.000080 +- 0.000000 ops/us | 0.000080 +- 0.000000 ops/us |

#### Async Profiler Metrics by Params/Mode
**cpu_jvm_user_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 9.995 | 9.995 |
| 100000 | 10.014 | 10.013 |
| 300000 | 10.001 | 10.004 |

**cpu_machine_total_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.414 | 10.413 |
| 100000 | 10.664 | 10.449 |
| 300000 | 10.458 | 10.662 |

**cpu_samples_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 101.849 | 101.807 |
| 100000 | 102.073 | 102.094 |
| 300000 | 102.500 | 102.260 |

**alloc_events_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 9.656 | 9.516 |
| 100000 | 9.406 | 8.995 |
| 300000 | 8.891 | 9.880 |

**alloc_events_total**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 232 | 228 |
| 100000 | 226 | 216 |
| 300000 | 213 | 237 |

**alloc_tlab_share_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 100.000 | 100.000 |
| 100000 | 100.000 | 100.000 |
| 300000 | 100.000 | 100.000 |

#### Notes
```text
- 
```

### removeThenPutGeoSpatialIndex

<img src="plots/bench.GeoSpatialIndexBenchmark.removeThenPutGeoSpatialIndex__score.png" alt="score" width="72%">

#### JMH Results
**avgt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 0.119184 +- 0.001080 us/op | 0.118545 +- 0.001131 us/op |
| 100000 | 0.215727 +- 0.008854 us/op | 0.206293 +- 0.006739 us/op |
| 300000 | 0.369371 +- 0.028320 us/op | 0.365688 +- 0.018865 us/op |

**thrpt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 8.430082 +- 0.124735 ops/us | 8.516366 +- 0.074304 ops/us |
| 100000 | 4.788463 +- 0.159167 ops/us | 4.723706 +- 0.176103 ops/us |
| 300000 | 2.824770 +- 0.363832 ops/us | 2.772078 +- 0.137996 ops/us |

#### Async Profiler Metrics by Params/Mode
**cpu_jvm_user_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.063 | 10.101 |
| 100000 | 11.186 | 10.709 |
| 300000 | 13.382 | 13.666 |

**cpu_machine_total_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.691 | 10.541 |
| 100000 | 11.554 | 11.233 |
| 300000 | 13.831 | 14.201 |

**cpu_samples_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 103.521 | 103.083 |
| 100000 | 114.438 | 107.833 |
| 300000 | 134.560 | 136.520 |

**alloc_events_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 1391.833 | 1402.875 |
| 100000 | 831.271 | 833.479 |
| 300000 | 523.900 | 518.400 |

**alloc_events_total**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 33404 | 33669 |
| 100000 | 19950 | 20004 |
| 300000 | 13098 | 12960 |

**alloc_tlab_share_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 100.000 | 100.000 |
| 100000 | 100.000 | 100.000 |
| 300000 | 100.000 | 100.000 |

#### Notes
```text
- 
```

### removeThenPutNaive

<img src="plots/bench.GeoSpatialIndexBenchmark.removeThenPutNaive__score.png" alt="score" width="72%">

#### JMH Results
**avgt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 0.026254 +- 0.000967 us/op | 0.022182 +- 0.003201 us/op |
| 100000 | 0.038907 +- 0.001796 us/op | 0.038731 +- 0.001186 us/op |
| 300000 | 0.083102 +- 0.007615 us/op | 0.081198 +- 0.008861 us/op |

**thrpt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 38.471954 +- 1.626216 ops/us | 38.289736 +- 1.220887 ops/us |
| 100000 | 25.612581 +- 1.403623 ops/us | 27.870566 +- 1.817064 ops/us |
| 300000 | 12.266564 +- 2.111176 ops/us | 11.671418 +- 0.749200 ops/us |

#### Async Profiler Metrics by Params/Mode
**cpu_jvm_user_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.510 | 10.549 |
| 100000 | 12.189 | 11.982 |
| 300000 | 18.471 | 17.205 |

**cpu_machine_total_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 11.112 | 11.233 |
| 100000 | 12.727 | 12.554 |
| 300000 | 19.170 | 17.790 |

**cpu_samples_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 106.458 | 107.333 |
| 100000 | 122.396 | 120.979 |
| 300000 | 178.260 | 166.080 |

**alloc_events_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 6398.104 | 6240.104 |
| 100000 | 4477.667 | 4256.229 |
| 300000 | 1818.220 | 1952.900 |

**alloc_events_total**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 153554 | 149762 |
| 100000 | 107464 | 102150 |
| 300000 | 45456 | 48822 |

**alloc_tlab_share_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 100.000 | 100.000 |
| 100000 | 100.000 | 100.000 |
| 300000 | 100.000 | 100.000 |

#### Notes
```text
- 
```

### upsertExistingGeoSpatialIndex

<img src="plots/bench.GeoSpatialIndexBenchmark.upsertExistingGeoSpatialIndex__score.png" alt="score" width="72%">

#### JMH Results
**avgt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 0.118135 +- 0.000878 us/op | 0.118005 +- 0.000827 us/op |
| 100000 | 0.193077 +- 0.004109 us/op | 0.199620 +- 0.005566 us/op |
| 300000 | 0.403837 +- 0.036569 us/op | 0.402623 +- 0.017347 us/op |

**thrpt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 8.454526 +- 0.074002 ops/us | 8.436241 +- 0.057624 ops/us |
| 100000 | 5.150097 +- 0.092101 ops/us | 5.097852 +- 0.108403 ops/us |
| 300000 | 2.494703 +- 0.308221 ops/us | 2.482845 +- 0.099825 ops/us |

#### Async Profiler Metrics by Params/Mode
**cpu_jvm_user_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.063 | 10.065 |
| 100000 | 10.494 | 10.576 |
| 300000 | 12.424 | 11.734 |

**cpu_machine_total_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.452 | 10.432 |
| 100000 | 10.872 | 11.030 |
| 300000 | 12.908 | 12.151 |

**cpu_samples_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 102.625 | 103.062 |
| 100000 | 107.708 | 108.229 |
| 300000 | 124.320 | 117.800 |

**alloc_events_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 913.958 | 904.125 |
| 100000 | 563.458 | 584.083 |
| 300000 | 343.640 | 331.260 |

**alloc_events_total**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 21935 | 21699 |
| 100000 | 13523 | 14018 |
| 300000 | 8591 | 8282 |

**alloc_tlab_share_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 100.000 | 100.000 |
| 100000 | 100.000 | 100.000 |
| 300000 | 100.000 | 100.000 |

#### Notes
```text
- 
```

### upsertExistingNaive

<img src="plots/bench.GeoSpatialIndexBenchmark.upsertExistingNaive__score.png" alt="score" width="72%">

#### JMH Results
**avgt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 0.014739 +- 0.000288 us/op | 0.014605 +- 0.000182 us/op |
| 100000 | 0.030242 +- 0.001587 us/op | 0.029868 +- 0.001701 us/op |
| 300000 | 0.056194 +- 0.005836 us/op | 0.058294 +- 0.002815 us/op |

**thrpt**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 68.604328 +- 1.156622 ops/us | 67.867834 +- 1.020715 ops/us |
| 100000 | 35.476320 +- 3.425025 ops/us | 33.559664 +- 1.580847 ops/us |
| 300000 | 18.645135 +- 4.521627 ops/us | 17.413719 +- 0.985508 ops/us |

#### Async Profiler Metrics by Params/Mode
**cpu_jvm_user_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 10.389 | 10.393 |
| 100000 | 11.116 | 11.097 |
| 300000 | 11.786 | 11.851 |

**cpu_machine_total_avg_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 11.093 | 10.837 |
| 100000 | 11.692 | 11.692 |
| 300000 | 12.343 | 12.387 |

**cpu_samples_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 104.979 | 104.208 |
| 100000 | 112.562 | 112.750 |
| 300000 | 117.800 | 119.360 |

**alloc_events_per_sec**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 7156.917 | 7141.625 |
| 100000 | 3672.958 | 4319.750 |
| 300000 | 1947.000 | 1833.760 |

**alloc_events_total**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 171766 | 171399 |
| 100000 | 88151 | 103674 |
| 300000 | 48675 | 45844 |

**alloc_tlab_share_pct**

| n \ cellSizeMeters | 120.0 | 500.0 |
|---|---|---|
| 10000 | 100.000 | 100.000 |
| 100000 | 100.000 | 100.000 |
| 300000 | 100.000 | 100.000 |

#### Notes
```text
- 
```
