from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AMARA").master("local[*]").getOrCreate()

project_root = Path(__file__).resolve().parents[2]

raw_file = (project_root/ "processing"/ "data"/ "staging"/ "rickowens"/ "20260830T204939Z/products.jsonl").resolve()

df = spark.read.json(str(raw_file))

df.printSchema()

print(f"Total records: {df.count()}")

df.select("id", "title", "vendor", "product_type", "site", "scraped_at").show(20, truncate=False)

df.select("vendor").distinct().show(truncate=False)

df.groupBy("vendor").count().orderBy("count", ascending=False).show()