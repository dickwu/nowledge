//! Compatibility façade for Nowledge's public model surface.
//!
//! Feature definitions live in flat sibling modules so the serialized API and
//! persisted records remain reviewable by domain. Re-export every item here to
//! preserve the established `nowledge::models::*` paths.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[path = "models_audit.rs"]
mod audit;
#[path = "models_common.rs"]
mod common;
#[path = "models_company_docs.rs"]
mod company_docs;
#[path = "models_context_rag_llm.rs"]
mod context_rag_llm;
#[path = "models_defaults.rs"]
mod defaults;
#[path = "models_harness_eval.rs"]
mod harness_eval;
#[path = "models_history.rs"]
mod history;
#[path = "models_ingest.rs"]
mod ingest;
#[path = "models_insights_links_analysis.rs"]
mod insights_links_analysis;
#[path = "models_operations.rs"]
mod operations;
#[path = "models_sessions.rs"]
mod sessions;
#[path = "models_state.rs"]
mod state;
#[path = "models_structured.rs"]
mod structured;

pub use audit::*;
pub use common::*;
pub use company_docs::*;
pub use context_rag_llm::*;
pub use defaults::*;
pub use harness_eval::*;
pub use history::*;
pub use ingest::*;
pub use insights_links_analysis::*;
pub use operations::*;
pub use sessions::*;
pub use state::*;
pub use structured::*;
