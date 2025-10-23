import polars as pl
from sqlalchemy import create_engine, text, Engine
from urllib.parse import quote


def mysql_engine() -> Engine:
    # host = "10.40.22.41"
    # user = "root"
    # pwd = quote("jHWnhLxg^qPG+5FNalgk")
    # db = "smart_prod_line_v1"
    host = "127.0.0.1"
    user = "root"
    pwd = "123"
    db = "playground"
    engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:3306/{db}")
    return engine


query = """
CREATE TABLE batch_indices_sidewall (
  pk INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
  line_id INTEGER NOT NULL,
  shift_name VARCHAR(25),
  front_start_datetime DATETIME,
  front_end_datetime DATETIME,
  front_norm_name VARCHAR(255),
  front_zl_standard FLOAT,
  front_zl_mean_op FLOAT,
  front_zl_cp_op FLOAT,
  front_zl_ca_op FLOAT,
  front_zl_cpk_op FLOAT,
  front_zl_rate_op FLOAT,
  front_zl_qualified_count_op INTEGER,
  front_zl_gt_usl_count_op INTEGER,
  front_zl_lt_lsl_count_op INTEGER,
  front_zl_valid_count_op INTEGER,
  front_zl_mean_mc FLOAT,
  front_zl_cp_mc FLOAT,
  front_zl_ca_mc FLOAT,
  front_zl_cpk_mc FLOAT,
  front_zl_rate_mc FLOAT,
  front_zl_qualified_count_mc INTEGER,
  front_zl_gt_usl_count_mc INTEGER,
  front_zl_lt_lsl_count_mc INTEGER,
  front_zl_valid_count_mc INTEGER,
  front_zk_standard FLOAT,
  front_zk_mean_op FLOAT,
  front_zk_cp_op FLOAT,
  front_zk_ca_op FLOAT,
  front_zk_cpk_op FLOAT,
  front_zk_rate_op FLOAT,
  front_zk_qualified_count_op INTEGER,
  front_zk_gt_usl_count_op INTEGER,
  front_zk_lt_lsl_count_op INTEGER,
  front_zk_valid_count_op INTEGER,
  front_zk_mean_mc FLOAT,
  front_zk_cp_mc FLOAT,
  front_zk_ca_mc FLOAT,
  front_zk_cpk_mc FLOAT,
  front_zk_rate_mc FLOAT,
  front_zk_qualified_count_mc INTEGER,
  front_zk_gt_usl_count_mc INTEGER,
  front_zk_lt_lsl_count_mc INTEGER,
  front_zk_valid_count_mc INTEGER,
  front_count INTEGER,
  behind_start_datetime DATETIME,
  behind_end_datetime DATETIME,
  behind_norm_name VARCHAR(255),
  behind_zl_standard FLOAT,
  behind_zl_mean_op FLOAT,
  behind_zl_cp_op FLOAT,
  behind_zl_ca_op FLOAT,
  behind_zl_cpk_op FLOAT,
  behind_zl_rate_op FLOAT,
  behind_zl_qualified_count_op INTEGER,
  behind_zl_gt_usl_count_op INTEGER,
  behind_zl_lt_lsl_count_op INTEGER,
  behind_zl_valid_count_op INTEGER,
  behind_zl_mean_mc FLOAT,
  behind_zl_cp_mc FLOAT,
  behind_zl_ca_mc FLOAT,
  behind_zl_cpk_mc FLOAT,
  behind_zl_rate_mc FLOAT,
  behind_zl_qualified_count_mc INTEGER,
  behind_zl_gt_usl_count_mc INTEGER,
  behind_zl_lt_lsl_count_mc INTEGER,
  behind_zl_valid_count_mc INTEGER,
  behind_zk_standard FLOAT,
  behind_zk_mean_op FLOAT,
  behind_zk_cp_op FLOAT,
  behind_zk_ca_op FLOAT,
  behind_zk_cpk_op FLOAT,
  behind_zk_rate_op FLOAT,
  behind_zk_qualified_count_op INTEGER,
  behind_zk_gt_usl_count_op INTEGER,
  behind_zk_lt_lsl_count_op INTEGER,
  behind_zk_valid_count_op INTEGER,
  behind_zk_mean_mc FLOAT,
  behind_zk_cp_mc FLOAT,
  behind_zk_ca_mc FLOAT,
  behind_zk_cpk_mc FLOAT,
  behind_zk_rate_mc FLOAT,
  behind_zk_qualified_count_mc INTEGER,
  behind_zk_gt_usl_count_mc INTEGER,
  behind_zk_lt_lsl_count_mc INTEGER,
  behind_zk_valid_count_mc INTEGER,
  behind_count INTEGER,
  control_rate FLOAT,
  id VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  extra_info TEXT
)
"""


query1 = """
CREATE TABLE control_front (
id VARCHAR(36) PRIMARY KEY  DEFAULT (UUID()),
line_no INTEGER,
start_time DATETIME,
end_time DATETIME,
std1 FLOAT,
std2 FLOAT,
spec VARCHAR(255),
plan_no VARCHAR(255)
)
"""

query_insert = """
INSERT INTO control_front (id, line_no, start_time, end_time, spec, std1, std2) 
VALUES (:id, :line_no, :start_time, :end_time, :spec, :std1, :std2);
"""

query_drop = "DROP TABLE control_front"


def sql_operation():
    df = pl.read_csv("/home/td/workspace/control_front.csv").select(
        "id",
        "line_no",
        pl.col("start_time").str.strptime(pl.Datetime("ms"), r"%d/%m/%Y %H:%M:%S"),
        pl.col("end_time").str.strptime(pl.Datetime("ms"), r"%d/%m/%Y %H:%M:%S"),
        "spec",
        "std1",
        "std2",
    )
    data_arr = df.to_dicts()
    # print(arr[0:15])
    engine = mysql_engine()
    with engine.connect() as conn:
        # conn.execute(text(query_drop))
        # conn.commit()
        # conn.execute(text(query1))
        # conn.commit()
        conn.execute(text(query_insert), data_arr)
        conn.commit()
        pass


if __name__ == "__main__":
    sql_operation()
