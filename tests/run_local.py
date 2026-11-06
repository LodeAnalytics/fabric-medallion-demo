"""
Run the four notebooks end to end on a local Spark session and check the results.

What it proves:
  * Bronze loads every file once. Re-running loads nothing new.
  * A second delivery updates existing silver rows in place (upsert) and adds new ones.
  * Bad rows (a value that won't cast, a line pointing at an unknown product) land in
    dq_quarantine with the checks they failed, and never reach silver or gold.
  * Weather loads incrementally per region from the last date already loaded.
  * Gold totals tie back to silver.

Two modes:
  * USE_DELTA=1 (what CI runs): real Delta tables and the notebooks' own Delta MERGE,
    on Spark 3.5 + Delta 3.2, the versions in Fabric Runtime 1.3.
  * Default: plain PySpark with no extra jars. Tables are written as Parquet and the
    Delta MERGE is swapped for an equivalent anti-join + union.
In both modes notebookutils.fs.ls and the Open-Meteo API are stubbed (the stub returns fake weather).

Usage (needs Java 17+):
    git clone https://github.com/lodeanalytics/demo-data-generator ../demo-data-generator
    pip install pyspark==3.5.3 delta-spark==3.2.1 nbformat
    USE_DELTA=1 python tests/run_local.py
"""

import csv
import importlib.util
import os
import random
import shutil
import sys
import tempfile
import types
from datetime import date, timedelta
from pathlib import Path

import nbformat
from pyspark.sql import SparkSession

REPO = Path(__file__).resolve().parents[1]
NOTEBOOKS = REPO / "notebooks"
GENERATOR = Path(os.environ.get("GENERATOR", REPO.parent / "demo-data-generator" / "generate_data.py"))
USE_DELTA = os.environ.get("USE_DELTA") == "1"

work = Path(tempfile.mkdtemp(prefix="medallion-test-"))
landing = work / "landing"

# ---------------------------------------------------------------------------
# Local stand-ins for Fabric
# ---------------------------------------------------------------------------

builder = (
    SparkSession.builder.master("local[2]")
    .appName("medallion-local-test")
    .config("spark.sql.warehouse.dir", str(work / "warehouse"))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
)
if USE_DELTA:
    from delta import configure_spark_with_delta_pip

    builder = (
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    builder = configure_spark_with_delta_pip(builder)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


class FileInfo:
    def __init__(self, path):
        self.path, self.name, self.isDir = str(path), path.name, path.is_dir()


notebookutils = types.SimpleNamespace(fs=types.SimpleNamespace(ls=lambda p: [FileInfo(c) for c in sorted(Path(p).iterdir())]))

weather_calls = []


def fake_weather_get(url, params, timeout):
    """Stands in for the Open-Meteo archive API: repeatable fake weather for the requested days."""
    weather_calls.append(params)
    start, end = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
    days = [(start + timedelta(d)).isoformat() for d in range((end - start).days + 1)]
    rng = random.Random(f"{params['latitude']}{params['start_date']}")
    highs = [round(rng.uniform(30, 100), 1) for _ in days]
    daily = {
        "time": days,
        "temperature_2m_max": highs,
        "temperature_2m_min": [round(h - rng.uniform(8, 25), 1) for h in highs],
        "precipitation_sum": [round(max(0.0, rng.gauss(0.05, 0.2)), 2) for _ in days],
    }
    return types.SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"daily": daily})


sys.modules["requests"] = types.SimpleNamespace(get=fake_weather_get)


def local_append(df, table):
    df.write.format("parquet").mode("append").saveAsTable(table)


def local_overwrite(df, table):
    # Materialize first: Spark won't overwrite a Parquet table it is still reading from.
    df.localCheckpoint().write.format("parquet").mode("overwrite").saveAsTable(table)


def local_upsert(df, table, keys):
    """Same result as the Delta MERGE in 00_utils: replace rows whose keys match, insert the rest."""
    if not spark.catalog.tableExists(table):
        local_overwrite(df, table)
        return
    kept = spark.table(table).join(df.select(*keys), keys, "left_anti")
    local_overwrite(kept.unionByName(df), table)


OVERRIDES = {"LANDING_ROOT": str(landing), "WEATHER_START": "2026-01-01"}
if not USE_DELTA:
    OVERRIDES.update(append=local_append, overwrite=local_overwrite, upsert=local_upsert)


def run_notebook(name):
    print(f"\n=== {name} ===")
    scope = {"spark": spark, "notebookutils": notebookutils, "__name__": "__notebook__"}
    for cell in nbformat.read(NOTEBOOKS / f"{name}.ipynb", as_version=4).cells:
        if cell.cell_type != "code":
            continue
        if cell.source.strip() == "%run 00_utils":
            for utils_cell in nbformat.read(NOTEBOOKS / "00_utils.ipynb", as_version=4).cells:
                if utils_cell.cell_type == "code":
                    exec(utils_cell.source, scope)
            scope.update(OVERRIDES)
            continue
        exec(cell.source, scope)
    return scope


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

def load_generator():
    if not GENERATOR.exists():
        sys.exit(f"Can't find {GENERATOR}. Clone demo-data-generator next to this repo, or set GENERATOR=path/to/generate_data.py")
    spec = importlib.util.spec_from_file_location("generate_data", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def first_delivery(gen):
    """January 2026, straight from the generator."""
    rng = random.Random(7)
    start, end = date(2026, 1, 1), date(2026, 1, 31)
    products = gen.make_products(rng)
    customers = gen.make_customers(rng)
    headers, lines = gen.make_sales(rng, customers, products, start, end)
    inventory = gen.make_inventory(rng, products, start, end)
    runs, downtime = gen.make_production(rng, products, start, end)
    drop = landing / "2026-01-31"
    for name, rows in [("products", products), ("customers", customers), ("sales_order_headers", headers),
                       ("sales_order_lines", lines), ("inventory_snapshots", inventory),
                       ("production_runs", runs), ("downtime_events", downtime)]:
        write_csv(drop / f"{name}.csv", rows)
    return products, headers, lines


def second_delivery(products, headers):
    """A price change, one good new order line, two bad ones, and a file the pipeline doesn't know."""
    drop = landing / "2026-02-07"
    changed = [dict(p) for p in products]
    changed[0]["case_price"] = round(float(changed[0]["case_price"]) + 5, 2)
    write_csv(drop / "products.csv", changed)

    order_no, item = headers[0]["order_no"], products[1]
    good = {"order_no": order_no, "line_no": 99, "item_no": item["item_no"], "cases": 10,
            "unit_price": item["case_price"], "discount_pct": 0, "net_amount": round(10 * item["case_price"], 2)}
    bad_cases = dict(good, line_no=98, cases="abc")
    bad_item = dict(good, line_no=97, item_no="PF-9999")
    write_csv(drop / "sales_order_lines.csv", [good, bad_cases, bad_item])
    write_csv(drop / "notes.csv", [{"note": "not a pipeline entity"}])
    return changed[0]


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

failures = []


def check(label, condition):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}")
    if not condition:
        failures.append(label)


def count(table):
    return spark.table(table).count()


print(f"Mode: {'Delta (real MERGE)' if USE_DELTA else 'Parquet (MERGE stand-in)'}, Spark {spark.version}")
gen = load_generator()
products, headers, lines = first_delivery(gen)
weather_days = (date.today() - timedelta(days=5) - date(2026, 1, 1)).days + 1

# Run 1: first delivery
run_notebook("01_bronze_ingest")
run_notebook("02_silver_transform")
run_notebook("03_gold_model")

print("\n--- checks after the first delivery ---")
check("bronze_sales_order_lines has every source row", count("bronze_sales_order_lines") == len(lines))
check("silver_sales_order_lines has every source row", count("silver_sales_order_lines") == len(lines))
check("load log has 7 files", count("etl_load_log") == 7)
check("weather loaded for 5 regions from the start date", count("bronze_weather_daily") == 5 * weather_days)
check("nothing quarantined from clean data", not spark.catalog.tableExists("dq_quarantine"))
gold_net = spark.table("fact_sales").agg({"net_amount": "sum"}).first()[0]
silver_net = spark.table("silver_sales_order_lines").agg({"net_amount": "sum"}).first()[0]
check("gold sales total ties to silver", gold_net == silver_net)
oee = spark.sql("SELECT sum(good_cases) / sum(ideal_cases_at_planned_time) AS oee FROM fact_production").first().oee
check(f"plant OEE is plausible ({oee:.1%})", 0.5 < oee < 0.95)

# Run 2: nothing new has arrived
calls_before = len(weather_calls)
run_notebook("01_bronze_ingest")
run_notebook("02_silver_transform")

print("\n--- checks after re-running with nothing new ---")
check("re-run loaded no files twice", count("bronze_sales_order_lines") == len(lines) and count("etl_load_log") == 7)
check("re-run made no weather API calls", len(weather_calls) == calls_before)
check("re-run left silver unchanged", count("silver_sales_order_lines") == len(lines))

# Run 3: second delivery
changed_product = second_delivery(products, headers)
run_notebook("01_bronze_ingest")
run_notebook("02_silver_transform")
run_notebook("03_gold_model")

print("\n--- checks after the second delivery ---")
check("unknown file skipped", count("etl_load_log") == 9)
check("silver_products still has one row per item", count("silver_products") == len(products))
new_price = spark.table("silver_products").where(f"item_no = '{changed_product['item_no']}'").first().case_price
check(f"price change applied by upsert ({new_price})", float(new_price) == changed_product["case_price"])
check("good new line added to silver", count("silver_sales_order_lines") == len(lines) + 1)
quarantine = {r.line_no: r.failed_checks for r in spark.sql(
    "SELECT get_json_object(record, '$.line_no') AS line_no, failed_checks FROM dq_quarantine").collect()}
check("line with cases='abc' quarantined as a failed cast", "cases_is_int" in quarantine.get("98", ""))
check("line with an unknown product quarantined", "item_no_exists_in_silver_products" in quarantine.get("97", ""))
check("bad lines never reached gold", count("fact_sales") == len(lines) + 1)
check("dq_results recorded the quarantined rows",
      spark.sql("SELECT sum(rows_quarantined) AS n FROM dq_results").first().n == 2)

if USE_DELTA:
    check("silver tables are Delta", spark.sql("DESCRIBE DETAIL silver_products").first().format == "delta")
    operations = [r.operation for r in spark.sql("DESCRIBE HISTORY silver_products").collect()]
    check("price change was written by a Delta MERGE", "MERGE" in operations)

print("\nSample of gold.fact_sales_weather_daily:")
spark.table("fact_sales_weather_daily").where("net_amount > 0").orderBy("date", "region").show(5)
print("dq_quarantine:")
spark.table("dq_quarantine").select("entity", "failed_checks", "record").show(truncate=80)

spark.stop()
shutil.rmtree(work, ignore_errors=True)
print(f"\n{'ALL CHECKS PASSED' if not failures else f'{len(failures)} CHECK(S) FAILED'}")
sys.exit(1 if failures else 0)
