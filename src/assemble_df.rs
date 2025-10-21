use crate::CONFIG_META;
use polars::prelude::*;

fn identity_batch(prefix: &str, by: &[String]) -> Expr {
    let batch_id = int_range(lit(0), len(), 1, DataType::Int64);
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

fn rectify_standard(indices: &str) -> Expr {
    todo!()
}

fn validate(indices: &str, limit: &(f32, f32, bool)) -> Expr {
    /*
    公差：宽度＜400mm以下±4mm；≥400mm以上±10mm；计算合格率米重标准±3%，有效米重数据采集超大限设±5%
    宽度有效<400mm: ±10mm, ≥400mm: ±15mm
    limit: (tolerace, valid, by_percentage)
    */
    todo!()
}

fn caculation_exprs(segments: Vec<String>) -> Vec<Expr> {
    // segments => vec![prefix, indices, suffix]
    // with caculation => vec![prefix, indices, caculation, suffix]
    // USL: Upper Specification Limit
    // LSL: Lower Specification Limit
    // μ(mu): Mean
    // σ(sigma): Standard Variance for sample
    // CP = (USL - LSL) / (6σ)
    // CA = μ - (USL + LSL) / 2
    // CPK = min( (USL - μ)/(3σ), (μ - LSL)/(3σ) )

    let col_name = &format!("{}{}{}", segments[0], segments[1], segments[2]);
    let splice = |cacu| format!("{}{}{}{}", segments[0], segments[1], cacu, segments[2]);
    let mean = col(col_name).mean().alias(splice("mean"));
    let stdv = col(col_name).std(0).alias(splice("std"));
    vec![mean, stdv]
}

pub fn assemble(df_front: LazyFrame, df_sw: LazyFrame) -> LazyFrame {
    let meta = &CONFIG_META;
    let cacu = meta.get_caculate();
    let limit_map = meta.get_limit();

    /*batch expression should be applied to lazyframe first */
    let mut _df_lazy = df_sw.with_columns(
        cacu["prefix"]
            .iter()
            .map(|prf| identity_batch(prf, &cacu["batch_by"]))
            .collect::<Vec<_>>(),
    );
    println!("{:?}\n****1***", _df_lazy.clone().collect().unwrap());

    _df_lazy = _df_lazy
        .join(
            df_front,
            [col("dt"), col("front_norm_name")],
            [col("dt"), col("front_norm_name")],
            JoinArgs::new(JoinType::Left),
        )
        .sort(["line_id", "dt"], Default::default());
    println!("{:?}\n****1***", _df_lazy.clone().collect().unwrap());

    _df_lazy = _df_lazy.with_columns(
        cacu["prefix"]
            .iter()
            .flat_map(|prf| {
                cacu["indices"].iter().flat_map(|indi| {
                    cacu["suffix"].iter().flat_map(|suf| {
                        let segments = vec![prf.to_string(), indi.to_string(), suf.to_string()];
                        caculation_exprs(segments)
                    })
                })
            })
            .collect::<Vec<_>>(),
    );
    _df_lazy
    // todo!()
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
            Local.with_ymd_and_hms(2025, 10, 15, 0, 0, 0).unwrap(),
            Local.with_ymd_and_hms(2025, 10, 18, 0, 0, 0).unwrap(),
        );
        let df_front = ControlFront::load_data_frame(&s, &e);
        let data_pool = DataFrameGenerator::new(line, &s, &e);
        for df in data_pool {
            let _df_sw = assemble(df_front.clone(), df);
            // let df_eager = df_sw.collect().unwrap();
            // println!("{df_eager}");
            break;
        }
    }
}
