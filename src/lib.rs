mod assemble_df;
mod config_meta;
mod diesel_model;
mod diesel_query;
mod guilun_qushu;
mod schema;

use chrono::{Local, TimeZone};
use config_meta::CONFIG_META;
use diesel_model::{ControlFront, Sidewall};
use diesel_query::Crud;
use guilun_qushu::DataFrameGenerator;

pub fn data_processing() {
    let meta = &CONFIG_META;
    let all_lines = meta.get_lines();
    let (overall_start, overall_end) = meta.get_init_end_point();

    for line in &all_lines {
        let line_id = meta.get_line_id(line);
        let sw_opt = Sidewall::load_scalar(line_id);
        let start = match sw_opt {
            Some(sw) => Local.from_local_datetime(&sw.dt.unwrap()).unwrap(),
            None => overall_start,
        };
        let df_front = ControlFront::load_data_frame(&start, &overall_end);
        let df_generator = DataFrameGenerator::new(line, &start, &overall_end);

        for df_sw in df_generator {
            let df_ready = assemble_df::assemble(df_front.clone(), df_sw);
            Sidewall::write_database(line_id, df_ready);
        }
    }
}
