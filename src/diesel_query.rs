use crate::diesel_model::{ControlFront, NewSidewall, Sidewall};
use chrono::{DateTime, Duration, Local, NaiveDateTime};
use diesel::{associations::HasTable, mysql::Mysql, prelude::*};
use dotenvy::dotenv;
use polars::prelude::{self as pl, DataFrame, LazyFrame};
use std::env;
use std::io::Cursor;

type DT = DateTime<Local>;

fn establish_connection() -> MysqlConnection {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    MysqlConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}

pub trait LoadDataFrame {
    type Item;
    fn load_data_frame(s: &DT, e: &DT) -> Self::Item;
}

impl LoadDataFrame for ControlFront {
    type Item = DataFrame;
    fn load_data_frame(s: &DT, e: &DT) -> Self::Item {
        /// model使用了HasQuery宏, 这里可以用query查询, 目前的简化不明显,
        /// 如果是web后端大量进行find方法的主键查询, 会带来很大便利
        use crate::schema::control_front::dsl::*;
        let conn = &mut establish_connection();

        let results: Vec<Self> = Self::query()
            .filter(dt.ge(&s.naive_local()))
            .filter(dt.lt(&e.naive_local()))
            // .limit(5)
            .load(conn)
            .expect("Error loading control_front!");
        if results.len() == 0 {
            return Default::default();
        } else {
            let col_vector: Vec<pl::Column> = vec![
                pl::Column::new(
                    "dt".into(),
                    results
                        .iter()
                        .map(|ctrlf| ctrlf.dt)
                        .collect::<Vec<NaiveDateTime>>(),
                ),
                pl::Column::new(
                    "end_datetime".into(),
                    results
                        .iter()
                        .map(|ctrlf| ctrlf.end_datetime)
                        .collect::<Vec<NaiveDateTime>>(),
                ),
                pl::Column::new(
                    "zl_s".into(),
                    results.iter().map(|ctrlf| ctrlf.zl_s).collect::<Vec<f32>>(),
                ),
                pl::Column::new(
                    "zk_s".into(),
                    results.iter().map(|ctrlf| ctrlf.zk_s).collect::<Vec<f32>>(),
                ),
                pl::Column::new(
                    "norm_name".into(),
                    results
                        .iter()
                        .map(|ctrlf| ctrlf.norm_name.to_owned())
                        .collect::<Vec<String>>(),
                ),
            ];
            return DataFrame::new(col_vector).unwrap();
        }
    }
}

impl LoadDataFrame for Sidewall {
    type Item = DataFrame;
    fn load_data_frame(_s: &DT, _e: &DT) -> Self::Item {
        Default::default() // todo!()
    }
}

impl Sidewall {
    pub fn load_scalar(line_id: i32) -> Option<Self> {
        use crate::schema::sidewall as sw;
        use diesel::dsl::max;

        let conn = &mut establish_connection();
        let max_pk = sw::table
            .filter(sw::line_id.eq(line_id))
            .select(max(sw::pk))
            .first::<Option<i32>>(conn)
            .unwrap()
            .unwrap_or(0);
        let result = sw::table
            .filter(sw::pk.eq(max_pk))
            .select(Sidewall::as_select())
            .first::<Self>(conn);
        if let Ok(sw_ins) = result {
            return Some(sw_ins);
        } else {
            return None;
        }
    }
}

impl NewSidewall {
    pub fn write_database(line_id: i32, df: LazyFrame) {
        use polars::prelude::*;

        let sw_pre = Sidewall::load_scalar(line_id);
        let mut df_cur = DataFrame::default();
        if sw_pre.is_none() {
            df_cur = df.collect().unwrap();
        } else {
            df_cur = df
                .filter(
                    col("behind_start_datetime")
                        .gt_eq(lit(sw_pre.unwrap().dt.unwrap_or(Default::default()))),
                )
                .collect()
                .unwrap();
        }
        // let mut df_cur = df.collect().unwrap();
        let mut buf = Vec::new();
        let cursor = Cursor::new(&mut buf);
        let _ = JsonWriter::new(cursor)
            .with_json_format(JsonFormat::Json)
            .finish(&mut df_cur)
            .unwrap();
        let json_str = str::from_utf8(&buf).expect("invalid json string!");
        let new_sw: Vec<NewSidewall> = serde_json::from_str(json_str).unwrap();
        // println!("{:?}", new_sw);
        // println!("\n{}", json_str);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::{df, prelude::IntoLazy};
    #[test]
    fn query_works() {
        // let sw = Sidewall::load_scalar(104);
        // println!("{:?}", sw);
        let df = df!(
            "line_id" => &[1, 2, 3],
            "front_norm_name" => &[Some("bak"),None,  Some("baz")],
        )
        .unwrap()
        .lazy();
        NewSidewall::write_database(104, df);
    }
}
