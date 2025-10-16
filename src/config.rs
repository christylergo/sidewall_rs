use chrono::{DateTime, Local};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;
use std::{env, fs};
use urlencoding::encode;

pub static CONFIG_META: LazyLock<Meta> = LazyLock::new(load_config);

#[derive(Debug, Deserialize)]
pub struct Meta {
    #[serde(rename = "BASIC")]
    basic: Basic,
    #[serde(rename = "TAG")]
    tags: HashMap<String, HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct Basic {
    qushu: QuShu,
    db_info: MySqlDataBase,
    line_info: HashMap<String, HashMap<String, String>>,
    calculate: Calculate,
    schema: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct QuShu {
    host: String,
    header: HashMap<String, String>,
    start: DateTime<Local>,
    end: DateTime<Local>,
}

#[derive(Debug, Deserialize)]
struct MySqlDataBase {
    host: String,
    user: String,
    pwd: String,
    db: String,
    rdbms: String,
}

#[derive(Debug, Deserialize)]
struct Calculate {
    indices: Vec<String>,
    batch_by: Vec<String>,
    filter: Vec<String>,
}

type DT = DateTime<Local>;

impl Meta {
    pub fn emit_qushu_url(&self, line: &str, s: &DT, e: &DT) -> String {
        let host = &self.basic.qushu.host;
        let tags = self.tags[line]
            .keys()
            .cloned()
            .into_iter()
            .collect::<Vec<String>>()
            .join(",");
        // "%Y/%m/%d %H:%M:%S"
        let (ss, ee) = (
            s.format("%Y/%m/%d %H:%M:%S").to_string(),
            e.format("%Y/%m/%d %H:%M:%S").to_string(),
        );
        let url = format! {"{}{}&stime={}&etime={}",host,tags, ss, ee};
        url
    }

    pub fn emit_database_url(&self) -> String {
        // mysql://username:password@localhost/diesel_demo
        let db = &self.basic.db_info;
        format!(
            "mysql://{}:{}@{}/{}",
            db.user,
            encode(&db.pwd),
            db.host,
            db.db
        )
    }
    pub fn get_header(&self) -> (&str, &str) {
        let header = self.basic.qushu.header.iter().next().unwrap();
        (&header.0, &header.1) //直接返回元组会提示类型不匹配,因为deref不会在元组中递归生效
    }
    pub fn get_tag_key(&self, tag: &str) -> &str {
        for (_, tk_map) in &self.tags {
            let Some(x) = tk_map.get(tag) else {
                continue;
            };
            return x;
        }
        return "";
    }
    pub fn get_line_id(&self, line: &str) -> i64 {
        self.basic.line_info[line]["line_id"].parse().unwrap()
    }
    pub fn get_schema(&self, tag: &str) -> &str {
        let key = self.get_tag_key(tag);
        for (key_segment, schema) in &self.basic.schema {
            if key.contains(key_segment) {
                return schema;
            }
        }
        return "";
    }
}

fn load_config() -> Meta {
    let raw_path = format!(
        "{}/{}",
        env::var("HOME").unwrap(),
        ".config/sidewall/sidewall_configs.toml"
    );
    let config_path = Path::new(&raw_path);
    // let config_path = Path::new("src/sidewall_tags.toml");
    let toml_str = fs::read_to_string(config_path).unwrap();
    let meta: Meta = toml::from_str(&toml_str).unwrap();

    return meta;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "already tested."]
    fn config_works() {
        let ss = &CONFIG_META;
        println!("{:?}", ss.basic.qushu);
        println!("{:?}", ss.basic.calculate);
    }
}
