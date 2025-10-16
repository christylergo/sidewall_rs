use chrono::NaiveDateTime;
use diesel::prelude::*;
use polars::prelude::{DataFrame, LazyFrame};

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::control_front)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct ControlFront {
    pub id: String,
    pub start_datetime: NaiveDateTime,
    pub end_datetime: NaiveDateTime,
    // #[diesel(column_name = "std1")]  // schema中已经进行了转换, 所以这里不用指定转换了
    pub zl_standard: f64,
    pub zk_standard: f64,
}
