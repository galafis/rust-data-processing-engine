// Logging utilities
// Author: Gabriel Demetrios Lafis

use log::{Level, LevelFilter, Metadata, Record, SetLoggerError};

/// Initialize logging with the given level
pub fn init_logging(level: LevelFilter) -> Result<(), SetLoggerError> {
    log::set_boxed_logger(Box::new(SimpleLogger))
        .map(|()| log::set_max_level(level))
}

/// Simple logger implementation
struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }
    
    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let level_str = match record.level() {
                Level::Error => "\x1B[31mERROR\x1B[0m",
                Level::Warn => "\x1B[33mWARN\x1B[0m",
                Level::Info => "\x1B[32mINFO\x1B[0m",
                Level::Debug => "\x1B[34mDEBUG\x1B[0m",
                Level::Trace => "\x1B[90mTRACE\x1B[0m",
            };
            
            println!("[{}] {}: {}", 
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                level_str,
                record.args()
            );
        }
    }
    
    fn flush(&self) {}
}

