def from_files(spark, file_format, data_dir):
    df = spark.read. \
        format(file_format). \
        option("header", True). \
        load(data_dir)
    return df
