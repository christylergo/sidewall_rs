use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::Deserialize;

/// 如果有大量的数据表需要映射查询, 目前diesel提供的功能使用时过于繁琐,
/// 可通过自定义derive macro来实现通用的查询功能, diesel重点关注
/// 类型检查, 没有直接将model和table关联起来, 所以代码中需要独立
/// 使用, 如果在model struct上实现通用查询方法, 就需要能够通过model
/// 关联获取到table, 这个功能需要自定义过程宏来实现. 由于宏代码难度
/// 较大, 暂时先实现查询功能, 以后再优化代码

#[derive(Debug, HasQuery)]
#[diesel(table_name = crate::schema::control_front)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct ControlFront {
    pub dt: NaiveDateTime,
    pub end_datetime: NaiveDateTime,
    pub zl_s: f32,
    pub zk_s: f32,
    pub norm_name: String,
}

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = crate::schema::sidewall)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct Sidewall {
    pub pk: i32,
    pub line_id: i32,
    #[column_name = "behind_start_datetime"]
    pub dt: Option<NaiveDateTime>,
    #[column_name = "behind_end_datetime"]
    pub end_datetime: Option<NaiveDateTime>,
    #[column_name = "behind_norm_name"]
    pub norm_name: Option<String>,
}

#[derive(Debug, Default, Deserialize, Insertable, Selectable)]
#[diesel(table_name = crate::schema::sidewall)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct NewSidewall {
    pub pk: i32,
    pub line_id: i32,
    pub shift_name: Option<String>,
    pub front_start_datetime: Option<NaiveDateTime>,
    pub front_end_datetime: Option<NaiveDateTime>,
    pub front_norm_name: Option<String>,
    pub front_zl_standard: Option<f32>,
    pub front_zl_mean_op: Option<f32>,
    pub front_zl_cp_op: Option<f32>,
    pub front_zl_ca_op: Option<f32>,
    pub front_zl_cpk_op: Option<f32>,
    pub front_zl_rate_op: Option<f32>,
    pub front_zl_qualified_count_op: Option<i32>,
    pub front_zl_gt_usl_count_op: Option<i32>,
    pub front_zl_lt_lsl_count_op: Option<i32>,
    pub front_zl_valid_count_op: Option<i32>,
    pub front_zl_mean_mc: Option<f32>,
    pub front_zl_cp_mc: Option<f32>,
    pub front_zl_ca_mc: Option<f32>,
    pub front_zl_cpk_mc: Option<f32>,
    pub front_zl_rate_mc: Option<f32>,
    pub front_zl_qualified_count_mc: Option<i32>,
    pub front_zl_gt_usl_count_mc: Option<i32>,
    pub front_zl_lt_lsl_count_mc: Option<i32>,
    pub front_zl_valid_count_mc: Option<i32>,
    pub front_zk_standard: Option<f32>,
    pub front_zk_mean_op: Option<f32>,
    pub front_zk_cp_op: Option<f32>,
    pub front_zk_ca_op: Option<f32>,
    pub front_zk_cpk_op: Option<f32>,
    pub front_zk_rate_op: Option<f32>,
    pub front_zk_qualified_count_op: Option<i32>,
    pub front_zk_gt_usl_count_op: Option<i32>,
    pub front_zk_lt_lsl_count_op: Option<i32>,
    pub front_zk_valid_count_op: Option<i32>,
    pub front_zk_mean_mc: Option<f32>,
    pub front_zk_cp_mc: Option<f32>,
    pub front_zk_ca_mc: Option<f32>,
    pub front_zk_cpk_mc: Option<f32>,
    pub front_zk_rate_mc: Option<f32>,
    pub front_zk_qualified_count_mc: Option<i32>,
    pub front_zk_gt_usl_count_mc: Option<i32>,
    pub front_zk_lt_lsl_count_mc: Option<i32>,
    pub front_zk_valid_count_mc: Option<i32>,
    pub front_count: Option<i32>,
    pub front_control_count: Option<i32>,
    pub behind_start_datetime: Option<NaiveDateTime>,
    pub behind_end_datetime: Option<NaiveDateTime>,
    pub behind_norm_name: Option<String>,
    pub behind_zl_standard: Option<f32>,
    pub behind_zl_mean_op: Option<f32>,
    pub behind_zl_cp_op: Option<f32>,
    pub behind_zl_ca_op: Option<f32>,
    pub behind_zl_cpk_op: Option<f32>,
    pub behind_zl_rate_op: Option<f32>,
    pub behind_zl_qualified_count_op: Option<i32>,
    pub behind_zl_gt_usl_count_op: Option<i32>,
    pub behind_zl_lt_lsl_count_op: Option<i32>,
    pub behind_zl_valid_count_op: Option<i32>,
    pub behind_zl_mean_mc: Option<f32>,
    pub behind_zl_cp_mc: Option<f32>,
    pub behind_zl_ca_mc: Option<f32>,
    pub behind_zl_cpk_mc: Option<f32>,
    pub behind_zl_rate_mc: Option<f32>,
    pub behind_zl_qualified_count_mc: Option<i32>,
    pub behind_zl_gt_usl_count_mc: Option<i32>,
    pub behind_zl_lt_lsl_count_mc: Option<i32>,
    pub behind_zl_valid_count_mc: Option<i32>,
    pub behind_zk_standard: Option<f32>,
    pub behind_zk_mean_op: Option<f32>,
    pub behind_zk_cp_op: Option<f32>,
    pub behind_zk_ca_op: Option<f32>,
    pub behind_zk_cpk_op: Option<f32>,
    pub behind_zk_rate_op: Option<f32>,
    pub behind_zk_qualified_count_op: Option<i32>,
    pub behind_zk_gt_usl_count_op: Option<i32>,
    pub behind_zk_lt_lsl_count_op: Option<i32>,
    pub behind_zk_valid_count_op: Option<i32>,
    pub behind_zk_mean_mc: Option<f32>,
    pub behind_zk_cp_mc: Option<f32>,
    pub behind_zk_ca_mc: Option<f32>,
    pub behind_zk_cpk_mc: Option<f32>,
    pub behind_zk_rate_mc: Option<f32>,
    pub behind_zk_qualified_count_mc: Option<i32>,
    pub behind_zk_gt_usl_count_mc: Option<i32>,
    pub behind_zk_lt_lsl_count_mc: Option<i32>,
    pub behind_zk_valid_count_mc: Option<i32>,
    pub behind_count: Option<i32>,
    pub behind_control_count: Option<i32>,
}
