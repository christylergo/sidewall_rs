// src/main.rs
use env_logger::Builder;
use log::LevelFilter;

fn main() {
    // 初始化 env_logger（全局生效）
    Builder::new()
        .filter(None, LevelFilter::Info)
        .try_init()
        .expect("日志初始化失败");

    // 调用其他模块
    // my_module::do_something();
}
