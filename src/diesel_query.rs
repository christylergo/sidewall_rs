use crate::diesel_model::ControlFront;

use ::polars::prelude::*;
use chrono::{DateTime, Duration, Local, NaiveDateTime};
use diesel::prelude::*;

type DT = DateTime<Local>;

pub fn establish_connection() -> MysqlConnection {
    let database_url = "database_url";
    MysqlConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}

fn demo() {
    use crate::schema::control_front::dsl::*;
}
