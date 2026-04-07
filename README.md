# YCSB RavenDB Binding

A YCSB binding for RavenDB that communicates directly with its **REST HTTP API**.
No official RavenDB Java client dependency required.

Compatible with **RavenDB 5.x, 6.x, 7.x** running in unsecured HTTP mode.

---

## Prerequisites

- Java 11+
- Maven 3.6+
- YCSB 0.17.0 unpacked
- RavenDB running locally (unsecured, HTTP)

---

## 1. Build the binding JAR

```bash
cd ycsb-ravendb
mvn clean package -q
```

This produces `target/ravendb-binding-0.17.0.jar` (fat JAR with Jackson bundled).

---

## 2. Copy the JAR into YCSB

```bash
# From inside ycsb-ravendb/
cp target/ravendb-binding-0.17.0.jar ../ycsb-0.17.0/lib/
```

---

## 3. Register the binding in YCSB

Edit `ycsb-0.17.0/bin/ycsb` (Python script) and add `ravendb` to the `DATABASES` dict:

```python
DATABASES = {
    ...
    "ravendb": "com.yahoo.ycsb.db.RavenDBClient",
}
```

---

## 4. Start RavenDB and create the database

```bash
# macOS — from the RavenDB extracted folder:
./Server/Raven.Server --ServerUrl=http://0.0.0.0:8080

# Then in another terminal, create the 'ycsb' database:
curl -X PUT http://localhost:8080/admin/databases \
  -H "Content-Type: application/json" \
  -d '{"DatabaseName": "ycsb", "Settings": {}, "Disabled": false}'
```

---

## 5. Configure workloads

Make sure all your workload files include these lines:

```
fieldcount=20
fieldlength=500
```

Or pass them directly via `-p`:
```bash
-p fieldcount=20 -p fieldlength=500
```

---

## 6. Run the benchmark

### Load phase (insert data)

```bash
cd ycsb-0.17.0

bin/ycsb load ravendb -s \
  -P ../ycsb-ravendb/ravendb.properties \
  -P workloads/workloada \
  -p recordcount=100000 \
  -p fieldcount=20 \
  -p fieldlength=500 \
  -threads 1
```

### Run phase (execute workload)

```bash
bin/ycsb run ravendb -s \
  -P ../ycsb-ravendb/ravendb.properties \
  -P workloads/workloada \
  -p recordcount=100000 \
  -p operationcount=100000 \
  -p fieldcount=20 \
  -p fieldlength=500 \
  -threads 3
```

---

## 7. Full test script (all workloads A–H, all scales)

```bash
#!/bin/bash
# run_ravendb_benchmark.sh
# Runs all YCSB workloads for RavenDB at 3 record scales and 3 thread counts.
# Repeat 3 times per config and average the runtime manually.

YCSB_HOME=../ycsb-0.17.0
PROPS=../ycsb-ravendb/ravendb.properties
RESULTS_DIR=./results/ravendb
mkdir -p $RESULTS_DIR

WORKLOADS=(a b c d e f g h)
RECORDS=(100000 1000000 10000000)
THREADS=(1 3 6)

for RECORDS_N in "${RECORDS[@]}"; do
  for T in "${THREADS[@]}"; do

    # === LOAD PHASE ===
    echo ">>> LOAD: ${RECORDS_N} records"
    $YCSB_HOME/bin/ycsb load ravendb -s \
      -P $PROPS \
      -p recordcount=$RECORDS_N \
      -p fieldcount=20 \
      -p fieldlength=500 \
      -threads $T \
      > $RESULTS_DIR/load_${RECORDS_N}_t${T}.txt 2>&1

    # === RUN PHASE for each workload ===
    for WL in "${WORKLOADS[@]}"; do
      echo ">>> RUN: workload${WL} | ${RECORDS_N} records | ${T} threads"
      $YCSB_HOME/bin/ycsb run ravendb -s \
        -P $PROPS \
        -P $YCSB_HOME/workloads/workload${WL} \
        -p recordcount=$RECORDS_N \
        -p operationcount=$RECORDS_N \
        -p fieldcount=20 \
        -p fieldlength=500 \
        -threads $T \
        > $RESULTS_DIR/run_${WL}_${RECORDS_N}_t${T}.txt 2>&1

      # Extract runtime from YCSB output
      RUNTIME=$(grep "RunTime(ms)" $RESULTS_DIR/run_${WL}_${RECORDS_N}_t${T}.txt \
                | awk '{print $3}')
      echo "  -> Runtime: ${RUNTIME} ms"
    done
  done
done

echo "=== All tests complete. Results in $RESULTS_DIR ==="
```

Make it executable and run:
```bash
chmod +x run_ravendb_benchmark.sh
./run_ravendb_benchmark.sh
```

---

## Configuration reference

| Property | Default | Description |
|---|---|---|
| `ravendb.url` | `http://localhost:8080` | RavenDB server base URL |
| `ravendb.database` | `ycsb` | Database name |
| `ravendb.collection` | `usertable` | Collection (table) name |
| `ravendb.timeout` | `10000` | HTTP connect+read timeout (ms) |
| `ravendb.batchsize` | `50` | Reserved for future bulk-insert support |

---

## API endpoints used

| YCSB operation | HTTP method | RavenDB endpoint |
|---|---|---|
| `read` | `GET` | `/databases/{db}/docs?id={col}/{key}` |
| `insert` | `PUT` | `/databases/{db}/docs?id={col}/{key}` |
| `update` | `PATCH` | `/databases/{db}/docs?id={col}/{key}` |
| `delete` | `DELETE` | `/databases/{db}/docs?id={col}/{key}` |
| `scan` | `POST` | `/databases/{db}/queries` (RQL) |

---

## Notes

- **Scan semantics**: RavenDB does not have a native sequential scan like key-value stores.
  The scan operation issues an RQL query ordered by document ID with the start key as a lower bound.
  This is the closest equivalent to YCSB's scan workload (E) in a document database.

- **Security**: This binding assumes RavenDB is running in unsecured HTTP mode, suitable for local benchmarking.
  For secured setups, add certificate headers to `openConnection()` in `RavenDBClient.java`.

- **Workloads G and H**: These are custom workloads not included in the standard YCSB distribution.
  Create them manually in the `workloads/` folder — see the report appendix for their definition.
