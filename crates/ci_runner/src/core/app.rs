//! Application initialization and setup

use crate::config::Config;
use crate::models::error::ExecutionError;
use crate::services::executor::ExecutorConfig;
use crate::services::scheduler::JobScheduler;
use crate::utils::workspace::WorkspaceManager;
use std::sync::Arc;

/// Application components
pub struct App {
    pub scheduler: Arc<JobScheduler>,
    pub workspace_manager: Arc<WorkspaceManager>,
}

impl App {
    /// Initialize application components
    pub async fn initialize(config: &Config) -> Result<Self, ExecutionError> {
        // Initialize workspace manager
        let workspace_manager = Arc::new(WorkspaceManager::new(
            config.executor.workspace_root.clone(),
        ));

        // Initialize job scheduler
        let scheduler = Arc::new(JobScheduler::new(config.executor.max_concurrent_jobs));

        Ok(App {
            scheduler,
            workspace_manager,
        })
    }

    /// Create executor config from application config
    pub fn create_executor_config(config: &Config) -> ExecutorConfig {
        let memory_limit = parse_memory_limit(&config.executor.resources.memory_limit)
            .unwrap_or(4 * 1024 * 1024 * 1024); // Default 4GB

        ExecutorConfig {
            cpu_limit: config.executor.resources.cpu_limit,
            memory_limit,
            pids_limit: config.executor.resources.pids_limit as i64,
            network_mode: config
                .executor
                .docker
                .network_mode
                .as_ref()
                .cloned()
                .unwrap_or_else(|| "bridge".to_string()),
            default_timeout: config.executor.timeouts.default,
            max_timeout: config.executor.timeouts.max,
            workspace_root: config.executor.workspace_root.clone(),
        }
    }
}

/// Parse memory limit string (e.g., "4GB", "512MB") to bytes
pub fn parse_memory_limit(limit: &str) -> Option<u64> {
    let limit = limit.trim().to_lowercase();
    if let Some(mb_pos) = limit.find("mb") {
        limit[..mb_pos].parse::<u64>().ok().map(|v| v * 1024 * 1024)
    } else if let Some(gb_pos) = limit.find("gb") {
        limit[..gb_pos]
            .parse::<u64>()
            .ok()
            .map(|v| v * 1024 * 1024 * 1024)
    } else if let Some(kb_pos) = limit.find("kb") {
        limit[..kb_pos].parse::<u64>().ok().map(|v| v * 1024)
    } else {
        limit.parse::<u64>().ok()
    }
}
