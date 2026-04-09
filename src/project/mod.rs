/// Project system: detection, storage, and PART_OF edge management.

pub mod config;
mod parser;
mod store;

pub use config::{
    AiBlock, AppearanceBlock, FocusBlock, GitBlock, PathsBlock, ProjectBlock, ProjectConfig,
    TrackerBlock,
};
pub use parser::{ParseError, ProjectParser};
pub use store::{Project, ProjectStatus, ProjectStore};
