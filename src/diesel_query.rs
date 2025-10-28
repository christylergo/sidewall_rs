use crate::CONFIG_META;
use crate::diesel_model::{ControlFront, NewSidewall, Sidewall};
// use crate::schema::sidewall;
use chrono::{DateTime, Local, NaiveDateTime};
use diesel::prelude::*;
// use dotenvy::dotenv;
use polars::prelude::{
    self as pl, DataFrame, IntoLazy, JsonFormat, JsonWriter, LazyFrame, SerWriter,
};
// use std::env;
use std::io::Cursor;

type DT = DateTime<Local>;

fn establish_connection() -> MysqlConnection {
    // dotenv().ok();

    // let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let database_url = (&CONFIG_META).emit_database_url();
    MysqlConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}

pub trait Crud<Scalar = Self> {
    fn load_scalar(line_id: u32) -> Option<Scalar>;
    fn load_data_frame(line_id: u32, s: &DT, e: &DT) -> LazyFrame;
    fn write_database(line_id: u32, df: LazyFrame);
}

impl Crud for ControlFront {
    fn load_scalar(_line_id: u32) -> Option<ControlFront> {
        Default::default()
    }
    fn write_database(_line_id: u32, _df: LazyFrame) {}

    fn load_data_frame(line_idx: u32, s: &DT, e: &DT) -> LazyFrame {
        /// model使用了HasQuery宏, 这里可以用query查询, 目前的简化不明显,
        /// 如果是web后端大量进行find方法的主键查询, 会带来很大便利
        use crate::schema::control_front::dsl::*;

        let conn = &mut establish_connection();

        let results: Vec<Self> = Self::query()
            .filter(line_id.eq(line_idx as i32))
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
                    "control_end_dt".into(),
                    results
                        .iter()
                        .map(|ctrlf| ctrlf.control_end_dt)
                        .collect::<Vec<NaiveDateTime>>(),
                ),
                pl::Column::new(
                    "front_zl_standard".into(),
                    results
                        .iter()
                        .map(|ctrlf| ctrlf.front_zl_standard)
                        .collect::<Vec<f32>>(),
                ),
                pl::Column::new(
                    "front_zk_standard".into(),
                    results
                        .iter()
                        .map(|ctrlf| ctrlf.front_zk_standard)
                        .collect::<Vec<f32>>(),
                ),
                pl::Column::new(
                    "front_norm_name".into(),
                    results
                        .iter()
                        .map(|ctrlf| ctrlf.front_norm_name.to_owned())
                        .collect::<Vec<String>>(),
                ),
            ];
            return DataFrame::new(col_vector).unwrap().lazy();
        }
    }
}

impl Crud for Sidewall {
    fn load_data_frame(_line_id: u32, _s: &DT, _e: &DT) -> LazyFrame {
        Default::default() // todo!()
    }
    fn load_scalar(line_id: u32) -> Option<Sidewall> {
        use crate::schema::sidewall as sw;
        use diesel::dsl::max;

        let conn = &mut establish_connection();
        let max_pk = sw::table
            .filter(sw::line_id.eq(line_id as i32))
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

    fn write_database(line_id: u32, df: LazyFrame) {
        NewSidewall::write_database(line_id, df);
    }
}

impl NewSidewall {
    fn translate(df: LazyFrame) -> Vec<NewSidewall> {
        let mut df_cur = df.collect().unwrap();
        // println!("{:?}", df_cur);
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
        return new_sw;
    }

    fn write_database(line_id: u32, df: LazyFrame) {
        let sw_pre = Sidewall::load_scalar(line_id);
        let sw_vector = Self::translate(df);
        let conn = &mut establish_connection();
        {
            use crate::schema::sidewall::dsl::*;

            let (pkey, dt, end_datetime, _norm_name) = if sw_pre.is_none() {
                Default::default()
            } else {
                let sw = &sw_pre.unwrap();
                (
                    sw.pk,
                    sw.dt.unwrap(),
                    sw.end_datetime.unwrap(),
                    sw.norm_name.clone().unwrap(),
                )
            };
            let new_sw_vec: &Vec<NewSidewall> = &sw_vector
                .into_iter()
                .filter(|inner_sw| inner_sw.behind_start_datetime.unwrap() >= dt)
                .collect();
            if new_sw_vec.len() < 1 {
                return;
            }

            let sw_first = &new_sw_vec[0];

            // 需要判断区间交界处的批次是否要更新或者丢弃
            let new_sw_slice: &[NewSidewall];

            if sw_first.behind_start_datetime.unwrap() == dt {
                if sw_first.behind_end_datetime.unwrap() > end_datetime {
                    // update unfinished previous sidewall batch
                    let _ = diesel::update(sidewall)
                        .filter(pk.eq(pkey))
                        .set(sw_first)
                        .execute(conn);
                    // todo!(); //其他更新项
                }
                if new_sw_vec.len() < 2 {
                    return;
                }
                new_sw_slice = &new_sw_vec[1..];
            } else {
                if sw_first.behind_start_datetime.unwrap() < end_datetime {
                    if new_sw_vec.len() < 2 {
                        return;
                    }
                    new_sw_slice = &new_sw_vec[1..];
                } else {
                    new_sw_slice = &new_sw_vec;
                }
            }
            // insert into sidewall table
            let _ = diesel::insert_into(sidewall)
                .values(new_sw_slice)
                .execute(conn)
                .unwrap();
        };
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
