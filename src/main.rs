// src/main.rs
use simplelog::{CombinedLogger, Config, LevelFilter, TermLogger, WriteLogger};
use std::fs::File;

fn main() {
    //初始化 logger

    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Info, // 终端只输出 info 及以上级别
            Config::default(),
            simplelog::TerminalMode::Stdout, // 输出到标准输出
            simplelog::ColorChoice::Auto,    // 自动决定是否启用彩色输出
        ),
        WriteLogger::new(
            LevelFilter::Info, // 文件输出 debug 及以上
            Config::default(),
            File::create("sidewall.log").unwrap(),
        ),
    ])
    .unwrap();

    guilun_sidewall::data_processing();
}
