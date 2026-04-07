#!/bin/bash
# =============================================================================
# run_all_benchmarks.sh  — YCSB Benchmark Runner
# Compatible with macOS bash 3.x
# =============================================================================

YCSB=./ycsb-0.17.0/bin/ycsb
RESULTS=./results
mkdir -p "$RESULTS"

WORKLOADS=(a b c d e f g h)
RECORDS=(100000 1000000 10000000)
THREADS=(1 3 6)
RUNS=3
FIELD_OPTS="-p fieldcount=20 -p fieldlength=500"

info()    { echo "[INFO]  $1"; }
warn()    { echo "[WARN]  $1"; }
section() { echo ""; echo "=============================="; echo " $1"; echo "=============================="; }

get_runtime_ms() {
    local val
    val=$(grep "RunTime(ms)" "$1" 2>/dev/null | awk '{print $3}' | tr -d ',' | tr -d '[:space:]')
    echo "${val:-0}"
}

get_throughput() {
    local val
    val=$(grep "Throughput(ops/sec)" "$1" 2>/dev/null | awk '{print $3}' | tr -d ',' | tr -d '[:space:]')
    echo "${val:-0}"
}

# Write average helper once
cat > /tmp/_avg.py << 'PYEOF'
import sys
vals = [float(x) for x in sys.argv[1:] if x and x != '0']
print(round(sum(vals)/len(vals), 2) if vals else 0)
PYEOF

average_ms() { python3 /tmp/_avg.py "$@"; }

init_csv() {
    echo "Workload,Records,Threads,AvgRuntime_ms,AvgRuntime_s,Throughput_ops_sec" > "$1"
}

append_csv() {
    local FILE=$1 WL=$2 REC=$3 THR=$4 RT_MS=$5 TP=$6
    local RT_S
    RT_S=$(python3 -c "print(round($RT_MS/1000, 2))" 2>/dev/null || echo "0")
    echo "${WL},${REC},${THR},${RT_MS},${RT_S},${TP}" >> "$FILE"
}

run_ycsb() {
    local PHASE=$1 BINDING=$2 WORKLOAD=$3 RECORDS_N=$4 THREADS_N=$5
    local EXTRA_OPTS=$6 LOG_PREFIX=$7
    local RT_VALS=() TP_VALS=()

    for i in $(seq 1 $RUNS); do
        local LOG="${LOG_PREFIX}_run${i}.txt"
        if [ "$PHASE" = "load" ]; then
            $YCSB load "$BINDING" -s $FIELD_OPTS $EXTRA_OPTS \
                -p recordcount="$RECORDS_N" -threads "$THREADS_N" > "$LOG" 2>&1
        else
            $YCSB run "$BINDING" -s -P "$WORKLOAD" $FIELD_OPTS $EXTRA_OPTS \
                -p recordcount="$RECORDS_N" -p operationcount="$RECORDS_N" \
                -threads "$THREADS_N" > "$LOG" 2>&1
        fi
        local RT TP
        RT=$(get_runtime_ms "$LOG")
        TP=$(get_throughput "$LOG")
        [ "$RT" = "0" ] && warn "No runtime in $LOG"
        RT_VALS+=("$RT")
        TP_VALS+=("$TP")
    done

    echo "$(average_ms "${RT_VALS[@]}")|$(average_ms "${TP_VALS[@]}")"
}

# $1=label $2=binding $3=csv_name $4=dir_name $5...=opts
run_db() {
    local LABEL=$1 BINDING=$2 CSV_NAME=$3 DIR_NAME=$4
    shift 4
    local OPTS="$*"
    local CSV="$RESULTS/${CSV_NAME}_results.csv"
    local DIR="$RESULTS/${DIR_NAME}"
    mkdir -p "$DIR"; init_csv "$CSV"
    section "$LABEL"

    for R in "${RECORDS[@]}"; do
        for T in "${THREADS[@]}"; do
            info "$LABEL | LOAD | ${R} records | ${T} threads"
            run_ycsb load "$BINDING" "./ycsb-0.17.0/workloads/workloada" "$R" "$T" "$OPTS" "$DIR/load_${R}_t${T}" > /dev/null
            for WL in "${WORKLOADS[@]}"; do
                info "$LABEL | workload${WL} | ${R} records | ${T} threads"
                local RESULT RT TP
                RESULT=$(run_ycsb run "$BINDING" \
                    "./ycsb-0.17.0/workloads/workload${WL}" \
                    "$R" "$T" "$OPTS" "$DIR/run_${WL}_${R}_t${T}")
                RT="${RESULT%%|*}"; TP="${RESULT##*|}"
                append_csv "$CSV" "workload${WL}" "$R" "$T" "$RT" "$TP"
                echo "    -> Runtime: ${RT} ms | Throughput: ${TP} ops/sec"
            done
        done
    done
    info "$LABEL complete -> $CSV"
}

generate_summary() {
    section "Summary"
    if [ -f "results_to_report.py" ]; then
        python3 results_to_report.py
    else
        echo "results_to_report.py not found — skipping summary"
    fi
}

if [ ! -f "$YCSB" ]; then
    echo "[ERROR] YCSB not found at $YCSB"
    echo "        Run this script from the folder containing ycsb-0.17.0/"
    exit 1
fi

echo ""
echo "============================================"
echo "  YCSB Benchmark Runner"
echo "============================================"
echo "  1) RavenDB"
echo "  2) MongoDB"
echo "  3) CouchDB"
echo "  4) MySQL"
echo "  5) All"
echo ""
read -rp "Enter choice(s) (e.g. 1 3): " -a CHOICES

RAN=0
for CHOICE in "${CHOICES[@]}"; do
    case "$CHOICE" in
        1) run_db "RavenDB"   "ravendb"   "ravendb"   "ravendb" \
               "-p ravendb.url=http://localhost:8080" \
               "-p ravendb.database=ycsb"
           RAN=1 ;;
        2) run_db "MongoDB" "mongodb" "mongodb" "mongodb" \
               "-p mongodb.url=mongodb://localhost:27017/ycsb"
           RAN=1 ;;
        3) run_db "CouchDB"   "couchdb"   "couchdb"   "couchdb" \
               "-p couchdb.url=http://localhost:5984/ycsb"
           RAN=1 ;;
        4) run_db "MySQL"     "jdbc"      "mysql"     "mysql" \
               "-p db.driver=com.mysql.cj.jdbc.Driver" \
               "-p db.url=jdbc:mysql://localhost:3306/ycsb" \
               "-p db.user=ycsb" \
               "-p db.passwd=ycsb_pass"
           RAN=1 ;;
        5) run_db "RavenDB"   "ravendb"   "ravendb"   "ravendb" \
               "-p ravendb.url=http://localhost:8080" "-p ravendb.database=ycsb"
           run_db "MongoDB" "mongodb" "mongodb" "mongodb" \
           run_db "CouchDB"   "couchdb"   "couchdb"   "couchdb" \
               "-p couchdb.url=http://localhost:5984/ycsb"
           run_db "MySQL"     "jdbc"      "mysql"     "mysql" \
               "-p db.driver=com.mysql.cj.jdbc.Driver" \
               "-p db.url=jdbc:mysql://localhost:3306/ycsb" \
               "-p db.user=ycsb" "-p db.passwd=ycsb_pass"
           RAN=1 ;;
        *) warn "Unknown choice: $CHOICE" ;;
    esac
done

if [ "$RAN" = "1" ]; then
    generate_summary
    echo ""
    echo "Done. Results saved to ./results/"
    ls -1 results/*.csv 2>/dev/null | sed 's/^/   /'
fi