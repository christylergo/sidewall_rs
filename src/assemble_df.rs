use crate::CONFIG_META;
use polars::prelude::*;

fn identity_batch(prefix: &str, by: &[String]) -> Expr {
    let batch_id = int_range(lit(0), len(), 1, DataType::UInt32);
    let batch_by: Vec<_> = by
        .iter()
        .cloned()
        .map(|s| col(format!("{prefix}_{s}")))
        .collect();
    // 可能会需要以早上8:30为界限划分
    // let eight_half = (col("dt") - lit(Duration::parse("8h30m"))).dt().date();
    // batch_by.push(eight_half);
    let raw_batch = batch_id.max().over(batch_by);
    let raw_batch_shifted = raw_batch.clone().shift(lit(1));
    let batch = raw_batch
        .neq_missing(raw_batch_shifted)
        .cum_sum(false)
        .alias(format!("{prefix}_batch"));
    return batch;
}

fn all_trimed(prefix: &str, trim_n: u32) -> Expr {
    let idx = int_range(lit(0), len(), 1, DataType::Int64);
    let idx_reverse = int_range(len() - lit(1), lit(-1), -1, DataType::Int64);
    let col_selector = format! {r"^{}.*$",prefix};
    let trim_expr = when(
        idx.gt_eq(trim_n)
            .and(idx_reverse.gt_eq(trim_n))
            .over([format!("{prefix}_batch").as_str()]),
    )
    .then(cols([col_selector]).as_expr())
    .otherwise(Null {}.lit());
    trim_expr
}

fn rectify_norm_name(prefix: &[String]) -> Vec<Expr> {
    prefix
        .iter()
        .map(|prf| {
            col(format!("{prf}_norm_name"))
                .str()
                // .extract(lit(r"^\d*(\w+\d+)$"), 1)
                .extract(lit(r"^(.+)$"), 1) // no action!
        })
        .collect()
}
fn identify_standard(segments_vec: Vec<Vec<String>>) -> Expr {
    let standard_col = &format!("{}_{}_standard", segments_vec[0][0], segments_vec[0][1]);
    let mut acc = lit(0_f32);
    let mut count = 0;
    for segments in &segments_vec {
        let col_name = &format!("{}_{}_{}", segments[0], segments[1], segments[2]);
        let raw_mean = col(col_name).mean(); //raw mean
        acc = acc + raw_mean;
        count += 1;
    }
    let bipartisan_mean = acc / lit(count);
    let standard = when(col(standard_col).is_not_null())
        .then(col(standard_col))
        .otherwise(bipartisan_mean)
        .mean()
        .over([format!("{}_batch", segments_vec[0][0]).as_str()])
        .alias(standard_col);

    return standard;
}

fn prelude_exprs(segments: Vec<String>, limit: &(f32, f32, bool)) -> Vec<Expr> {
    /*
    公差：宽度＜400mm以下±4mm；≥400mm以上±10mm；计算合格率米重标准±3%，有效米重数据采集超大限设±5%
    宽度有效<400mm: ±10mm, ≥400mm: ±15mm
    limit: (tolerace, valid, by_percentage)
    */
    // segments => vec![prefix, indices, suffix]
    // with calculation => vec![prefix, indices, calculation, suffix]
    // USL: Upper Specification Limit
    // LSL: Lower Specification Limit
    // μ(mu): Mean
    // σ(sigma): Standard Variance for sample

    let batch_col = &format!("{}_batch", segments[0]);
    let standard_col = &format!("{}_{}_standard", segments[0], segments[1]);
    let usl_col = &format!("{}_{}_usl", segments[0], segments[1]);
    let lsl_col = &format!("{}_{}_lsl", segments[0], segments[1]);
    let usl_valid_col = &format!("{}_{}_usl_valid", segments[0], segments[1]);
    let lsl_valid_col = &format!("{}_{}_lsl_valid", segments[0], segments[1]);

    let standard = col(standard_col);
    let usl = if limit.2 {
        (standard.clone() * lit(1.0 + limit.0)).alias(usl_col) // by percentage 
    } else {
        when(standard.clone().lt(400.0))
            .then(standard.clone() + lit(limit.0))
            .otherwise(standard.clone() + lit(10.0))
            .alias(usl_col)
    };
    let lsl = if limit.2 {
        (standard.clone() * lit(1.0 - limit.0)).alias(lsl_col) // by percentage 
    } else {
        when(standard.clone().lt(400.0))
            .then(standard.clone() - lit(limit.0))
            .otherwise(standard.clone() - lit(10.0))
            .alias(lsl_col)
    };
    let usl_valid = if limit.2 {
        (standard.clone() * lit(1.0 + limit.1)).alias(usl_valid_col) // by percentage 
    } else {
        when(standard.clone().lt(400.0))
            .then(standard.clone() + lit(limit.1))
            .otherwise(standard.clone() + lit(15.0))
            .alias(usl_valid_col)
    };
    let lsl_valid = if limit.2 {
        (standard.clone() * lit(1.0 - limit.1)).alias(lsl_valid_col) // by percentage 
    } else {
        when(standard.clone().lt(400.0))
            .then(standard.clone() - lit(limit.1))
            .otherwise(standard.clone() - lit(15.0))
            .alias(lsl_valid_col)
    };

    // 标准和上下限, op, mc侧共享一套, 不能重复
    let exprs = if segments[2] == "op" {
        vec![usl, lsl, usl_valid, lsl_valid]
    } else {
        vec![]
    };

    exprs
        .into_iter()
        .map(|raw_expr| {
            raw_expr
                .round(6, Default::default())
                .over([batch_col.as_str()])
        })
        .collect()
}

fn calculation_exprs(segments: Vec<String>) -> Vec<Expr> {
    // CP = (USL - LSL) / (6σ)
    // CA = μ - (USL + LSL) / 2
    // CPK = min( (USL - μ)/(3σ), (μ - LSL)/(3σ) )
    let col_name = &format!("{}_{}_{}", segments[0], segments[1], segments[2]);
    let splice = |cacu| format!("{}_{}_{}_{}", segments[0], segments[1], cacu, segments[2]);

    let batch_col = &format!("{}_batch", segments[0]);
    let standard_col = &format!("{}_{}_standard", segments[0], segments[1]);
    let usl_col = &format!("{}_{}_usl", segments[0], segments[1]);
    let lsl_col = &format!("{}_{}_lsl", segments[0], segments[1]);
    let usl_valid_col = &format!("{}_{}_usl_valid", segments[0], segments[1]);
    let lsl_valid_col = &format!("{}_{}_lsl_valid", segments[0], segments[1]);

    let qualified = when(
        col(col_name)
            .lt_eq(col(usl_col))
            .and(col(col_name).gt_eq(col(lsl_col))),
    )
    .then(col(col_name))
    .otherwise(Null {}.lit());
    let valid = when(
        col(col_name)
            .lt_eq(col(usl_valid_col))
            .and(col(col_name).gt_eq(col(lsl_valid_col))),
    )
    .then(col(col_name))
    .otherwise(Null {}.lit());

    let stdvar = valid.clone().std(0);
    let is_even = (int_range(lit(0), len(), 1, DataType::UInt32) % lit(2)).eq(0);

    let mean = valid
        .clone()
        .mean()
        .round(4, Default::default())
        .alias(splice("mean"));

    let cp = ((col(usl_col) - col(lsl_col)) / (lit(6) * stdvar.clone()))
        .fill_nan(Null {}.lit())
        .round(4, Default::default())
        .alias(splice("cp"));
    let ca = (mean.clone() - (col(usl_col) + col(lsl_col)) / lit(2))
        .fill_nan(Null {}.lit())
        .round(4, Default::default())
        .alias(splice("ca"));
    // 上下限对称, 可以使用简化的cpk公式
    let cpk = (cp.clone() - (mean.clone() - col(standard_col)).abs() / (lit(3) * stdvar.clone()))
        .fill_nan(Null {}.lit())
        .round(4, Default::default())
        .alias(splice("cpk"));

    let qualified_count = qualified.clone().count().alias(splice("qualified_count"));
    let gt_count = valid
        .clone()
        .gt(col(usl_col))
        .sum()
        .alias(splice("gt_usl_count"));
    let lt_count = valid
        .clone()
        .lt(col(lsl_col))
        .sum()
        .alias(splice("lt_lsl_count"));
    let valid_count = valid.clone().count().alias(splice("valid_count"));

    // let rate = (qualified_count.clone().cast(DataType::Float64)
    //     / valid_count.clone().cast(DataType::Float64))
    // .alias(splice("rate"));
    // 两条相加之和, 计算合格率
    let rate = {
        let valid_0 = when(is_even.clone())
            .then(valid.clone())
            .otherwise(Null {}.lit());
        let valid_1 = when(is_even.clone())
            .then(valid.clone().shift(lit(-1)))
            .otherwise(Null {}.lit());
        let valid_pair = valid_0 + valid_1;
        let is_qualified_pair = valid_pair
            .clone()
            .lt_eq(lit(2) * col(usl_col))
            .and(valid_pair.clone().gt_eq(lit(2) * col(lsl_col)));
        (col(col_name)
            .filter(is_qualified_pair)
            .count()
            .cast(DataType::Float64)
            / valid_pair.count().cast(DataType::Float64))
        .fill_nan(Null {}.lit())
        .round(6, Default::default())
        .alias(splice("rate"))
    };

    let all_count_and_control = if segments[1] == "zl" && segments[2] == "op" {
        let ac = col(col_name)
            .count()
            .alias(format!("{}_count", segments[0]));
        let control_rate = if segments[0] == "front" {
            let cc = col(col_name).filter(col("control")).count();
            vec![
                (cc.cast(DataType::Float64) / ac.clone().cast(DataType::Float64))
                    .fill_nan(Null {}.lit())
                    .round(6, Default::default())
                    .alias("control_rate"),
            ]
        } else {
            vec![]
        };
        vec![ac].into_iter().chain(control_rate.into_iter())
    } else {
        vec![].into_iter().chain(vec![].into_iter())
    };
    vec![
        mean,
        cp,
        ca,
        cpk,
        rate,
        qualified_count,
        gt_count,
        lt_count,
        valid_count,
    ]
    .into_iter()
    .chain(all_count_and_control)
    .map(|raw_expr| raw_expr.over([batch_col.as_str()]))
    .collect()
}

fn align_batch(df_lazy: LazyFrame) -> LazyFrame {
    // use chrono::TimeZone;
    // use chrono_tz::Asia::Shanghai as tz;

    let df_f = df_lazy
        .clone()
        // .with_column(col("dt").dt().convert_time_zone(TimeZone::from_chrono(&tz))) // 需要polars的TimeZone, 而不是chrono的
        .select([col("dt"), col("control_rate"), col(r"^front.+$")])
        .filter(col("front_batch").is_not_null())
        .group_by(["front_batch"])
        .agg([
            col("dt").first().alias("front_start_datetime"),
            col("dt").last().alias("front_end_datetime"),
            all().exclude_cols(["dt"]).as_expr().first(),
        ])
        .sort(["front_start_datetime"], Default::default());
    // println!("{:?}", df_f.clone().collect().unwrap());

    let df_b = df_lazy
        .clone()
        // .with_column(col("dt").dt().convert_time_zone(TimeZone::from_chrono(&tz)))
        .select([
            col("dt"),
            col("line_id"),
            // col("shift_name"),
            col(r"^behind.+$"),
        ])
        .filter(col("behind_batch").is_not_null())
        .group_by(["behind_batch"])
        .agg([
            col("dt").first().alias("behind_start_datetime"),
            col("dt").last().alias("behind_end_datetime"),
            all().exclude_cols(["dt"]).as_expr().first(),
        ])
        .sort(["behind_start_datetime"], Default::default());

    let df_f_eager = df_f.collect().unwrap();
    let df_b_eager = df_b.collect().unwrap();
    let df = df_b_eager
        .join_asof_by(
            &df_f_eager,
            "behind_end_datetime",
            "front_start_datetime",
            ["behind_norm_name"],
            ["front_norm_name"],
            AsofStrategy::Backward,
            Some(AnyValue::Duration(
                3 * 10 * 10 * 10 * 60 * 60,
                TimeUnit::Milliseconds,
            )),
            true,
            true,
        )
        .unwrap()
        .lazy()
        // .select([all().exclude_cols(["^.+sub$"]).as_expr()])
        .sort(["behind_start_datetime"], Default::default());

    // println!("{:?}", df.clone().collect().unwrap());

    df
}

pub fn assemble(df_front: LazyFrame, df_sw: LazyFrame) -> LazyFrame {
    let meta = &CONFIG_META;
    let cacu = meta.get_calculate();
    let limit_map = meta.get_limit();
    let origin_schema = df_sw.clone().collect_schema().unwrap();

    /*batch expression should be applied to lazyframe first */
    let df_lazy = df_sw
        .filter(col("status").and(col("line_velocity_real").gt(5))) // 基础剔除
        .with_columns(rectify_norm_name(&cacu["prefix"]))
        .group_by_dynamic(
            col("dt"),
            [],
            DynamicGroupOptions {
                every: Duration::parse("10s"), // 10秒窗口
                period: Duration::parse("10s"),
                offset: Duration::parse("0s"), // 不偏移
                ..Default::default()
            },
        )
        .agg([
            col("line_id").first(),
            dtype_col(&DataType::String).as_selector().as_expr().first(),
            dtype_col(&DataType::Float64).as_selector().as_expr().mean(),
            dtype_col(&DataType::Boolean)
                .as_selector()
                .as_expr()
                .any(true),
            dtype_col(&DataType::UInt32).as_selector().as_expr().first(),
        ])
        .sort(["dt"], Default::default())
        .with_columns(
            cacu["prefix"]
                .iter()
                .map(|prf| identity_batch(prf, &cacu["batch_by"]))
                .collect::<Vec<_>>(),
        )
        .with_columns([all_trimed("front", 30), all_trimed("behind", 30)]);
    println!("{:?}", df_lazy.clone().collect());

    let df_lazy_1 = df_lazy
        .join(
            df_front.with_columns(rectify_norm_name(&vec!["front".into()])),
            [col("dt"), col("front_norm_name")],
            [col("dt"), col("front_norm_name")],
            JoinArgs::new(JoinType::Left),
        )
        .sort(["dt"], Default::default())
        .with_column(
            cols(["control_end_dt", "front_zl_standard", "front_zk_standard"])
                .as_expr()
                .fill_null_with_strategy(FillNullStrategy::Forward(None))
                .over(["front_batch"]),
        )
        .with_column(
            when(col("control_end_dt").gt_eq(col("dt")))
                .then(cols(["control_end_dt", "front_zl_standard", "front_zk_standard"]).as_expr())
                .otherwise(Null {}.lit()),
        );
    // println!("{:?}", df_lazy_1.clone().collect());

    let df_lazy_2 = df_lazy_1
        .with_columns(
            cacu["prefix"]
                .iter()
                .flat_map(|prf| {
                    cacu["indices"].iter().filter_map(|indi| {
                        let mut segments_vec = Vec::new();
                        for suf in &cacu["suffix"] {
                            let segments = vec![prf.to_string(), indi.to_string(), suf.to_string()];
                            if origin_schema.contains(&segments.clone().join("_")) {
                                segments_vec.push(segments);
                            }
                        }
                        if segments_vec.len() > 0 {
                            Some(identify_standard(segments_vec))
                        } else {
                            None
                        }
                    })
                })
                .collect::<Vec<_>>(),
        )
        .with_columns(
            cacu["prefix"]
                .iter()
                .flat_map(|prf| {
                    cacu["indices"].iter().flat_map(|indi| {
                        cacu["suffix"]
                            .iter()
                            .filter_map(|suf| {
                                let segments =
                                    vec![prf.to_string(), indi.to_string(), suf.to_string()];
                                if origin_schema.contains(&segments.clone().join("_")) {
                                    Some(prelude_exprs(segments, &limit_map[indi]))
                                } else {
                                    None
                                }
                            })
                            .flatten()
                    })
                })
                .collect::<Vec<_>>(),
        );
    // println!("{:?}", df_lazy_2.clone().collect());

    let df_lazy_3 = df_lazy_2.with_columns(
        cacu["prefix"]
            .iter()
            .flat_map(|prf| {
                cacu["indices"].iter().flat_map(|indi| {
                    cacu["suffix"]
                        .iter()
                        .filter_map(|suf| {
                            let segments = vec![prf.to_string(), indi.to_string(), suf.to_string()];
                            if origin_schema.contains(&segments.clone().join("_")) {
                                Some(calculation_exprs(segments))
                            } else {
                                None
                            }
                        })
                        .flatten()
                })
            })
            .collect::<Vec<_>>(),
    );
    // println!("{:?}", df_lazy_3.clone().collect());
    // df_lazy_3

    // 计算前后端的zl,zk对应的cpk汇总均值 (op + mc) / 2
    // rate汇总平均 (qualified_op + qualified_mc) / (valid_op + valid_mc)
    let df_lazy_4 = df_lazy_3.with_columns(
        cacu["prefix"]
            .iter()
            .flat_map(|prf| {
                cacu["indices"]
                    .iter()
                    // there is no behind_zl tag, so kick it out
                    .filter_map(|indi| {
                        let segments =
                            vec![prf.to_string(), indi.to_string(), cacu["suffix"][0].clone()];
                        if origin_schema.contains(&segments.join("_")) {
                            Some(vec![
                                (cacu["suffix"].iter().fold(lit(0_f32), |acc, suf| {
                                    let cpk_col =
                                        format!("{}_{}_cpk_{}", prf.to_string(), indi, suf);
                                    acc + col(cpk_col)
                                }) / lit(2_f32))
                                .round(4, Default::default())
                                .alias(format!("{}_{}_cpk_avg", prf.to_string(), indi)),
                                (cacu["suffix"]
                                    .iter()
                                    .fold(lit(0_i32), |acc, suf| {
                                        let qualified_col = format!(
                                            "{}_{}_qualified_count_{}",
                                            prf.to_string(),
                                            indi,
                                            suf
                                        );
                                        acc + col(qualified_col)
                                    })
                                    .cast(DataType::Float64)
                                    / cacu["suffix"]
                                        .iter()
                                        .fold(lit(0_i32), |acc, suf| {
                                            let valid_col = format!(
                                                "{}_{}_valid_count_{}",
                                                prf.to_string(),
                                                indi,
                                                suf
                                            );
                                            acc + col(valid_col)
                                        })
                                        .cast(DataType::Float64))
                                .round(6, Default::default())
                                .fill_nan(Null {}.lit())
                                .alias(format!("{}_{}_rate_avg", prf.to_string(), indi)),
                            ])
                        } else {
                            None
                        }
                    })
                    .flatten()
            })
            .collect::<Vec<_>>(),
    );

    let df_ready = align_batch(df_lazy_4.clone());

    df_ready
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_model::ControlFront;
    use crate::diesel_query::Crud;
    use crate::guilun_qushu::DataFrameGenerator;
    use chrono::{Local, TimeZone};

    #[test]
    fn assemble_df_works() {
        let (line, s, e) = (
            "SW01",
            Local.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap(),
            Local.with_ymd_and_hms(2025, 6, 18, 0, 0, 0).unwrap(),
        );
        let df_front = ControlFront::load_data_frame(104, &s, &e);
        let data_pool = DataFrameGenerator::new(line, &s, &e);
        for df in data_pool {
            let _df_sw = assemble(df_front.clone(), df);
            // let df_eager = df_sw.collect().unwrap();
            // println!("{df_eager}");
            break;
        }
    }
}
