from pyspark.sql import DataFrame, SparkSession

from .spark import table_format


def exists(spark: SparkSession, path: str) -> bool:
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p)


def read(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format(table_format()).load(path)


def overwrite(df: DataFrame, path: str) -> None:
    df.write.format(table_format()).mode("overwrite").option("overwriteSchema", "true").save(path)
