use crate::diesel_model::{ControlFront, NewSidewall, Sidewall};

use ::polars::prelude::*;
use chrono::{DateTime, Duration, Local, NaiveDateTime};
use diesel::{mysql::Mysql, prelude::*};
use dotenvy::dotenv;
use std::env;

type DT = DateTime<Local>;

pub trait LoadDataFrame {
    type Item;
    fn load_data_frame(s: &DT, e: &DT) -> Self::Item;
}

impl LoadDataFrame for ControlFront {
    type Item = LazyFrame;
    fn load_data_frame(s: &DT, e: &DT) -> Self::Item {
        /// model使用了HasQuery宏, 这里可以用query查询, 目前的简化不明显,
        /// 如果是web后端大量进行find方法的主键查询, 会带来很大便利
        use crate::schema::control_front::dsl::*;
        let conn = &mut establish_connection();
        let naive_s = s.naive_local();
        let results: Vec<Self> = Self::query()
            .filter(dt.ge(&naive_s))
            .filter(dt.lt(&naive_s))
            .limit(5)
            .load(conn)
            .expect("Error loading posts");
        todo!()
    }
}

pub fn establish_connection() -> MysqlConnection {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    MysqlConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_works() {
        todo!()
        // fetch_data();
    }
}
