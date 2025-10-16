diesel::table! {
    control_front (id) {
        id -> Varchar,
        #[sql_name = "start_time"]  // 指定对应数据库中数据表的字段名
        start_datetime -> Datetime,
        #[sql_name = "end_time"]
        end_datetime -> Datetime,
        #[sql_name = "std1"]
        zl_standard -> Double,
        #[sql_name = "std2"]
        zk_standard -> Double,
    }
}
