from utils import get_spark_session

spark = get_spark_session()


def order(orders):
    df = spark.sql("""select * from orders""")
    return df
