/// Project system: detection, storage, PART_OF edges, and filesystem watching.

pub mod config;
pub mod emitter;
mod parser;
pub mod signals;
mod store;
pub mod watch_config;
pub mod watcher;

pub use config::{
    AiBlock, AppearanceBlock, FocusBlock, GitBlock, PathsBlock, ProjectBlock, ProjectConfig,
    TrackerBlock,
};
pub use parser::{ParseError, ProjectParser};
pub use signals::{DetectionSignal, SignalDetector, SignalType};
pub use store::{Project, ProjectStatus, ProjectStore};
pub use watch_config::WatchConfig;
