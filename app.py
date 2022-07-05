import os
from utils import get_spark_session
from reader import from_files
from process import order
from writer import to_files


def main():
    spark = get_spark_session()
    orders_path = os.environ.get('ORDERS_PATH')
    # print(orders_path)
    src_file_format = os.environ.get("SRC_FILE_FORMAT")
    # print(src_file_format)
    df_orders = from_files(spark, src_file_format, orders_path)
    orders = df_orders.createOrReplaceTempView("orders")
    df = order(orders)
    df.show()
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get("TGT_FILE_FORMAT")
    to_files(df, tgt_dir, tgt_file_format)


if __name__ == '__main__':
    main()
