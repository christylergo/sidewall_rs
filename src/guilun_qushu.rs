use crate::CONFIG_META;
use chrono::{DateTime, Duration, Local};
use diesel::dsl::json;
use log::{debug, error, info};
use polars::prelude::*;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, thread::sleep, time::Duration as time_duration};

static STEP: Duration = Duration::days(7); // 7天
static INTERVAL: Duration = Duration::hours(12); // 12小时, 取数小间隔
type DT = DateTime<Local>;

#[derive(Debug, Deserialize)]
struct RawStr {
    #[serde(rename = "Result")]
    data_map: HashMap<String, HashMap<String, Value>>,
}
#[derive(Debug, Deserialize)]
pub struct DataFrameGenerator {
    line: String,
    overall_start: DT,
    current_start: DT,
    overall_end: DT,
}
impl DataFrameGenerator {
    pub fn new(line: &str, overall_start: &DT, overall_end: &DT) -> Self {
        DataFrameGenerator {
            line: line.to_string(),
            overall_start: overall_start.clone(),
            current_start: *overall_start - STEP,
            overall_end: overall_end.clone().min(Local::now()),
        }
    }
}

impl Iterator for DataFrameGenerator {
    type Item = LazyFrame;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_start + STEP < self.overall_end {
            self.current_start += STEP;
            let s = self.current_start - INTERVAL;
            let e = self.overall_end.min(self.current_start + STEP);
            let df = emit_qushu_dataframe(&self.line, &s, &e);

            Some(df)
        } else {
            None
        }
    }
}

fn get_raw_str(line: &str, s: &DT, e: &DT) -> String {
    let meta = &CONFIG_META;
    let client = Client::new();
    let url = meta.emit_qushu_url(line, s, e);

    for _ in 0..=2 {
        let result = client
            .get(&url)
            .header(meta.get_header().0, meta.get_header().1)
            .send();
        match result {
            Ok(res) => {
                return res.text().unwrap();
            }
            Err(e) => {
                error!("{}", e.to_string());
                sleep(time_duration::from_secs(3));
            }
        };
    }
    let error_str = "someting wrong with the network!";
    error!("{}", error_str);
    return error_str.into();
}

fn datetime_range(s: &DT, e: &DT) -> HashMap<String, Value> {
    let mut dt_range = HashMap::new();
    let mut dt = s.clone();
    let every = Duration::seconds(1);
    // 闭区间
    while dt <= *e {
        let k = dt.format("%Y/%m/%d %H:%M:%S").to_string();
        dt_range.insert(k, Value::Null);
        dt += every;
    }
    dt_range
}

fn parse_str(raw_str: String, dt_index: &HashMap<String, Value>) -> DataFrame {
    let meta = &CONFIG_META;
    let data_map = serde_json::from_str::<RawStr>(&raw_str).unwrap().data_map;

    let mut data_vector: Vec<(&str, &str, Vec<(String, Value)>)> = data_map
        .into_iter()
        .map(|(tag, v)| {
            let mut tag_data = dt_index.clone();
            tag_data.extend(v);

            let mut tuple_arr: Vec<(String, Value)> = tag_data.into_iter().collect();
            tuple_arr.sort_by_key(|(k, _)| k.clone());

            (meta.get_tag_key(&tag), meta.get_schema(&tag), tuple_arr)
        })
        .collect();
    data_vector.sort_by_key(|(k, ..)| k.to_string()); // 涉及到复杂的闭包lifetime问题, 此处用clone代替引用

    let mut column_vector: Vec<Column> = data_vector
        .into_iter()
        .map(|(k, t, data)| match t {
            "str" => Column::new(
                k.into(),
                data.into_iter()
                    .map(|(_, d)| {
                        if let Some(d_str) = d.as_str() {
                            Some(d_str.to_string())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
            ),
            "f64" => Column::new(
                k.into(),
                data.into_iter()
                    .map(|(_, d)| d.as_f64())
                    .collect::<Vec<_>>(),
            ),
            "bool" => Column::new(
                k.into(),
                data.into_iter()
                    .map(|(_, d)| d.as_bool())
                    .collect::<Vec<_>>(),
            ),
            _ => Column::new(k.into(), vec![Option::<&str>::None]),
        })
        .collect();

    let mut dt = dt_index
        .iter()
        .map(|(dt, _)| dt)
        .cloned()
        .collect::<Vec<_>>();
    dt.sort();
    column_vector.push(Column::new("dt".into(), dt));

    DataFrame::new(column_vector).unwrap()
}

fn emit_qushu_dataframe(line: &str, start: &DT, end: &DT) -> LazyFrame {
    let mut df_vec: Vec<LazyFrame> = Vec::new();
    let mut dt = start.clone();
    while dt < *end {
        let e = (*end).min(dt + INTERVAL - Duration::seconds(1)); // 减去后端点, 避免重复
        let raw_str = get_raw_str(line, &dt, &e);
        let dt_index = datetime_range(&dt, &e); //闭区间
        let df = parse_str(raw_str, &dt_index);
        if !df.is_empty() {
            df_vec.push(df.lazy())
        };
        dt += INTERVAL;
    }

    if df_vec.len() > 0 {
        concat(df_vec, Default::default())
            .unwrap()
            .with_columns([
                col("dt").str().strptime(
                    DataType::Datetime(TimeUnit::Milliseconds, None),
                    // DataType::Datetime(
                    //     TimeUnit::Milliseconds,
                    //     Some(unsafe { TimeZone::from_static("Asia/Shanghai") }),
                    // ),
                    StrptimeOptions::default(),
                    lit("raise"),
                ),
                lit((&CONFIG_META).get_line_id(line)).alias("line_id"),
            ])
            .select([all()
                .as_expr()
                .fill_null_with_strategy(FillNullStrategy::Forward(None))])
            .filter(col("status").is_not_null())
            .sort(["dt"], Default::default())
    } else {
        LazyFrame::default()
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    // #[ignore = "already tested"]
    fn qushu_works() {
        let (line, s, e) = (
            "SW01",
            Local.with_ymd_and_hms(2025, 10, 15, 1, 1, 1).unwrap(),
            Local.with_ymd_and_hms(2025, 10, 19, 1, 1, 1).unwrap(),
        );

        let data_pool = DataFrameGenerator::new(line, &s, &e);
        for df in data_pool {
            println! {"{}", df.collect().unwrap()};
        }
    }
}
