use crate::models::error::{DockerError, ExecutionError};
use crate::models::types::{JobContext, JobResult, JobStatus, Step, StepResult, StepType};
use bollard::Docker;
use bollard::exec::{CreateExecOptions, StartExecOptions};
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters::{
    CreateContainerOptions, RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
};
use chrono::Utc;
use futures_util::StreamExt;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, instrument, warn};

pub struct JobExecutor {
    docker: Docker,
    config: ExecutorConfig,
    log_streamer: Option<std::sync::Arc<dyn LogStreamerTrait>>,
}

pub trait LogStreamerTrait: Send + Sync {
    fn send(&self, job_id: uuid::Uuid, run_id: uuid::Uuid, step_name: Option<String>, level: crate::models::types::LogLevel, message: &[u8]);
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub cpu_limit: f64,
    pub memory_limit: u64,
    pub pids_limit: i64,
    pub network_mode: String,
    pub default_timeout: Duration,
    pub max_timeout: Duration,
    pub workspace_root: PathBuf,
}

impl JobExecutor {
    pub fn new(docker: Docker, config: ExecutorConfig) -> Self {
        Self {
            docker,
            config,
            log_streamer: None,
        }
    }

    pub fn with_log_streamer(mut self, streamer: std::sync::Arc<dyn LogStreamerTrait>) -> Self {
        self.log_streamer = Some(streamer);
        self
    }

    #[instrument(skip(self), fields(job_id = %job.job_id))]
    pub async fn execute(&self, job: JobContext) -> Result<JobResult, ExecutionError> {
        info!("[EXEC] ===== Starting job execution for job_id: {} =====", job.job_id);
        info!("[EXEC] Workspace path: {}", job.workspace_path.display());
        info!("[EXEC] Docker image: {}:{}", job.config.image.name, job.config.image.tag);
        info!("[EXEC] Total steps configured: {}", job.config.steps.len());
        let start_time = Utc::now();

        // Verify workspace directory exists and has files before starting container
        info!("[EXEC] Verifying workspace directory exists: {}", job.workspace_path.display());
        if !job.workspace_path.exists() {
            return Err(ExecutionError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Workspace directory does not exist: {}", job.workspace_path.display())
            )));
        }
        
        // List workspace contents to verify files are present
        let mut entries = tokio::fs::read_dir(&job.workspace_path).await
            .map_err(|e| ExecutionError::IoError(std::io::Error::other(
                format!("Failed to read workspace directory: {}", e)
            )))?;
        
        let mut file_count = 0;
        let mut file_names = Vec::new();
        while let Some(entry) = entries.next_entry().await
            .map_err(|e| ExecutionError::IoError(std::io::Error::other(
                format!("Failed to read directory entry: {}", e)
            )))? {
            file_count += 1;
            if let Some(name) = entry.file_name().to_str() {
                file_names.push(name.to_string());
            }
        }
        
        info!("[EXEC] Workspace directory contains {} items: {:?}", file_count, file_names);
        
        if file_count == 0 {
            warn!("[EXEC] WARNING: Workspace directory is empty! Files may not have been cloned correctly.");
        }

        // Pull Docker image
        info!("[EXEC] Pulling Docker image: {}:{}", job.config.image.name, job.config.image.tag);
        self.pull_image(&job.config.image).await?;
        info!("[EXEC] Docker image pull completed");

        // Create container
        info!("[EXEC] Creating Docker container");
        let container_id = self.create_container(&job).await?;
        info!("[EXEC] Docker container created with ID: {}", container_id);

        // Start container - handle mount errors (Docker Desktop Mac limitation)
        info!("[EXEC] Starting Docker container: {}", container_id);
        if let Err(e) = self
            .docker
            .start_container(&container_id, None::<StartContainerOptions>)
            .await
        {
            warn!("[EXEC] Failed to start container: {}", e);
            let error_msg = e.to_string();
            if error_msg.contains("mounts denied")
                || error_msg.contains("not shared")
                || error_msg.contains("not known to Docker")
            {
                // Docker Desktop on Mac doesn't allow mounting from inside container to container
                // Clean up the container and return a clearer error
                let _ = self
                    .docker
                    .remove_container(&container_id, None::<RemoveContainerOptions>)
                    .await;
                return Err(ExecutionError::DockerError(
                    DockerError::ContainerCreationFailed(format!(
                        "Docker mount failed (Docker Desktop Mac limitation). Workspace path {} cannot be mounted from inside container. Consider using a Docker volume instead of bind mount.",
                        job.workspace_path.display()
                    )),
                ));
            }
            return Err(ExecutionError::DockerError(DockerError::ApiError(format!(
                "Failed to start container: {}",
                e
            ))));
        }

        // Ensure cleanup
        let cleanup_container = |id: &str| {
            let docker = self.docker.clone();
            let id = id.to_string();
            async move {
                if let Err(e) = docker
                    .stop_container(&id, None::<StopContainerOptions>)
                    .await
                {
                    warn!("Failed to stop container {}: {}", id, e);
                }
                if let Err(e) = docker
                    .remove_container(&id, None::<RemoveContainerOptions>)
                    .await
                {
                    warn!("Failed to remove container {}: {}", id, e);
                }
            }
        };

        info!("[EXEC] Docker container started successfully");
        info!("[EXEC] Container ID: {}", container_id);

        // Execute steps in order (pre -> exec -> post)
        info!("[EXEC] ===== Beginning step execution phase =====");
        let mut results = Vec::new();
        let mut job_failed = false;
        
        // Create evaluation context
        let mut eval_context = crate::utils::step_evaluator::StepEvaluationContext::from_job_context(&job);
        info!("[EXEC] Step evaluation context created");

        for step_type in [StepType::Pre, StepType::Exec, StepType::Post] {
            info!("[EXEC] ===== Starting {} steps phase =====", format!("{:?}", step_type));
            
            if job_failed && step_type == StepType::Post {
                // Still execute post steps even if pre/exec failed
                info!("[EXEC] Job has failed, but continuing with post steps");
            } else if job_failed {
                info!("[EXEC] Job has failed, skipping {} steps", format!("{:?}", step_type));
                continue;
            }

            let steps = self.get_steps_by_type(&job.config.steps, step_type);
            info!("[EXEC] Found {} {} steps to execute", steps.len(), format!("{:?}", step_type));

            for (name, step) in steps {
                info!("[EXEC] ===== Starting step: {} (type: {:?}) =====", name, step.step_type);
                
                // Evaluate if condition
                if let Some(ref if_condition) = step.if_condition {
                    match crate::utils::step_evaluator::StepEvaluator::evaluate_if_condition(
                        if_condition,
                        &eval_context,
                    ) {
                        Ok(should_run) => {
                            if !should_run {
                                info!(step = %name, condition = %if_condition, "Skipping step due to if condition");
                                continue;
                            }
                        }
                        Err(e) => {
                            warn!(step = %name, condition = %if_condition, error = %e, "Failed to evaluate if condition, skipping step");
                            continue;
                        }
                    }
                }
                
                // Evaluate when condition
                if let Some(ref when) = step.when {
                    let should_run = crate::utils::step_evaluator::StepEvaluator::should_run_when(when, job_failed);
                    info!("[EXEC] Step '{}' when condition evaluation: {:?} -> should_run: {}", name, when, should_run);
                    if !should_run {
                        info!("[EXEC] Step '{}' skipped due to when condition: {:?}", name, when);
                        continue;
                    }
                }
                
                // Execute step with retry logic
                let mut result = None;
                let max_attempts = step.retry.as_ref().map(|r| r.max_attempts).unwrap_or(1);
                
                for attempt in 1..=max_attempts {
                    if attempt > 1 {
                        if let Some(ref retry_policy) = step.retry {
                            let delay = crate::utils::step_evaluator::StepEvaluator::calculate_retry_delay(
                                retry_policy,
                                attempt,
                            );
                            info!(step = %name, attempt = attempt, delay_secs = delay.as_secs(), "Retrying step after delay");
                            tokio::time::sleep(delay).await;
                        }
                    }
                    
                    info!("[EXEC] Executing step '{}' (attempt {}/{})", name, attempt, max_attempts);
                    match self.execute_step(&container_id, &name, &step, &job).await {
                        Ok(step_result) => {
                            info!("[EXEC] Step '{}' completed with exit code: {}", name, step_result.exit_code);
                            if !step_result.stdout.is_empty() {
                                info!("[EXEC] Step '{}' stdout (last 500 chars): {}", name, 
                                      step_result.stdout.chars().rev().take(500).collect::<String>().chars().rev().collect::<String>());
                            }
                            if !step_result.stderr.is_empty() {
                                warn!("[EXEC] Step '{}' stderr (last 500 chars): {}", name,
                                      step_result.stderr.chars().rev().take(500).collect::<String>().chars().rev().collect::<String>());
                            }
                            
                            result = Some(step_result.clone());
                            
                            // If step succeeded or we're on last attempt, break
                            if step_result.exit_code == 0 || attempt == max_attempts {
                                if step_result.exit_code == 0 {
                                    info!("[EXEC] Step '{}' succeeded", name);
                                } else {
                                    warn!("[EXEC] Step '{}' failed after {} attempts", name, attempt);
                                }
                                break;
                            }
                            
                            // If continue_on_error is true, break even on failure
                            if step.continue_on_error {
                                info!("[EXEC] Step '{}' failed but continue_on_error is true, continuing", name);
                                break;
                            }
                        }
                        Err(e) => {
                            warn!("[EXEC] Step '{}' execution error: {}", name, e);
                            if attempt == max_attempts {
                                return Err(e);
                            }
                            warn!(step = %name, attempt = attempt, error = %e, "Step execution failed, will retry");
                        }
                    }
                }
                
                let step_result = result.expect("Step result should be set");
                results.push(step_result.clone());
                
                info!("[EXEC] Step '{}' finished - exit_code: {}, duration: {:?}", 
                      name, step_result.exit_code, step_result.finished_at - step_result.started_at);
                
                // Update evaluation context with this step's result
                eval_context.previous_steps.insert(name.clone(), step_result.clone());

                // Note: Artifact collection for post steps will be handled after step execution
                // in the main job handler, as we need access to the artifact store

                if step_result.exit_code != 0 && !step.continue_on_error {
                    warn!("[EXEC] Step '{}' failed with exit code {}, marking job as failed", name, step_result.exit_code);
                    job_failed = true;
                    if step_type != StepType::Post {
                        info!("[EXEC] Stopping execution due to step failure (not post step)");
                        break;
                    }
                }
            }
        }

        info!("[EXEC] ===== Step execution phase completed =====");
        info!("[EXEC] Total steps executed: {}", results.len());
        info!("[EXEC] Steps that failed: {}", results.iter().filter(|r| r.exit_code != 0).count());

        // Cleanup container
        info!("[EXEC] Cleaning up Docker container: {}", container_id);
        cleanup_container(&container_id).await;
        info!("[EXEC] Docker container cleanup completed");

        // Aggregate results
        let finished_at = Utc::now();
        let status = if results.iter().any(|r| r.exit_code != 0) {
            JobStatus::Failed
        } else {
            JobStatus::Success
        };
        
        info!("[EXEC] Job final status: {:?}", status);

        let result = JobResult {
            status,
            steps: results,
            started_at: start_time,
            finished_at,
        };

        info!(
            duration_ms = result.duration().as_millis(),
            status = ?result.status,
            "Job execution completed"
        );

        Ok(result)
    }

    async fn pull_image(&self, image: &crate::models::types::DockerImage) -> Result<(), ExecutionError> {
        let image_name = if let Some(ref registry) = image.registry {
            format!("{}/{}:{}", registry, image.name, image.tag)
        } else {
            format!("{}:{}", image.name, image.tag)
        };

        match image.pull_policy {
            crate::models::types::PullPolicy::Never => {
                info!("Skipping image pull (policy: Never)");
                return Ok(());
            }
            crate::models::types::PullPolicy::IfNotPresent => {
                // Check if image exists
                if self.docker.inspect_image(&image_name).await.is_ok() {
                    info!("Image {} already exists, skipping pull", image_name);
                    return Ok(());
                }
            }
            crate::models::types::PullPolicy::Always => {
                // Always pull
            }
        }

        info!("Pulling image: {}", image_name);

        use bollard::query_parameters::CreateImageOptions;
        let options = Some(CreateImageOptions {
            from_image: Some(image_name.clone()),
            ..Default::default()
        });

        let mut stream = self.docker.create_image(options, None, None);
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(_) => {
                    // Image pull progress
                }
                Err(e) => {
                    return Err(ExecutionError::DockerError(DockerError::ImagePullFailed(
                        format!("Failed to pull image {}: {}", image_name, e),
                    )));
                }
            }
        }

        Ok(())
    }

    async fn create_container(&self, job: &JobContext) -> Result<String, ExecutionError> {
        info!("[EXEC] Creating container for job: {}", job.job_id);
        let image_name = if let Some(ref registry) = job.config.image.registry {
            format!(
                "{}/{}:{}",
                registry, job.config.image.name, job.config.image.tag
            )
        } else {
            format!("{}:{}", job.config.image.name, job.config.image.tag)
        };
        info!("[EXEC] Container image: {}", image_name);

        let container_name = format!("ci-job-{}", job.job_id);
        info!("[EXEC] Container name: {}", container_name);

        // Clean up any existing container with the same name (from previous failed attempts)
        if let Ok(existing) = self
            .docker
            .inspect_container(
                &container_name,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await {
            if let Some(id) = existing.id.as_ref() {
                warn!("Found existing container {}, removing it", container_name);
                let _ = self
                    .docker
                    .stop_container(id, None::<StopContainerOptions>)
                    .await;
                let _ = self
                    .docker
                    .remove_container(id, None::<RemoveContainerOptions>)
                    .await;
            }
            }

        // Use bind mount so files cloned into the host workspace are visible in the job container
        let workspace_host_path = self.config.workspace_root.display().to_string();
        let host_config = HostConfig {
            cpu_quota: Some((self.config.cpu_limit * 100_000.0) as i64),
            cpu_period: Some(100_000),
            memory: Some(self.config.memory_limit as i64),
            memory_swap: Some(self.config.memory_limit as i64), // No swap
            pids_limit: Some(self.config.pids_limit),
            binds: Some(vec![format!("{}:/workspace:rw", workspace_host_path)]),
            network_mode: Some(self.config.network_mode.clone()),
            ..Default::default()
        };

        let mut env_vars = Vec::new();
        env_vars.push(format!("CI_JOB_ID={}", job.job_id));
        env_vars.push(format!("CI_RUN_ID={}", job.run_id));
        env_vars.push(format!("CI_REPO_OWNER={}", job.repository.owner));
        env_vars.push(format!("CI_REPO_NAME={}", job.repository.name));
        env_vars.push(format!("CI_COMMIT_SHA={}", job.repository.commit_sha));
        env_vars.push(format!("CI_REF_NAME={}", job.repository.ref_name));

        // Add global env vars
        for (key, value) in &job.config.global_env {
            env_vars.push(format!("{}={}", key, value));
        }

        // Set working directory to the job-specific subdirectory within the volume
        // NOTE: We do NOT set working_dir in the container config because Docker may create
        // an empty directory if it doesn't exist, which would hide the files in the volume.
        // Instead, we'll cd into the directory in each exec command.
        let job_workspace_dir = format!("/workspace/{}", job.job_id);
        info!("[EXEC] Container workspace directory (will use in exec commands): {}", job_workspace_dir);
        info!("[EXEC] Environment variables count: {}", env_vars.len());

        // Use a command that keeps the container running so we can exec into it
        // Alpine and most base images exit immediately without a command
        let keep_alive_cmd = vec![
            "sh".to_string(),
            "-c".to_string(),
            "trap 'exit 0' TERM; while :; do sleep 1; done".to_string(),
        ];
        info!("[EXEC] Container keep-alive command: {:?}", keep_alive_cmd);

        let config = ContainerCreateBody {
            image: Some(image_name),
            cmd: Some(keep_alive_cmd),
            // working_dir: None, // Don't set working_dir - let exec commands handle it
            env: Some(env_vars),
            host_config: Some(host_config),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            tty: Some(false),
            ..Default::default()
        };

        let options = Some(CreateContainerOptions {
            name: Some(container_name.clone()),
            platform: "linux".to_string(),
        });

        // Try to create container, handle name conflicts
        let result = match self
            .docker
            .create_container(options.clone(), config.clone())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("already in use") || error_str.contains("Conflict") {
                    // Try cleanup and retry once
                    warn!("Container name conflict detected, attempting cleanup and retry");
                    if let Ok(existing) = self
                        .docker
                        .inspect_container(
                            &container_name,
                            None::<bollard::query_parameters::InspectContainerOptions>,
                        )
                        .await
                    {
                        if let Some(id) = existing.id.as_ref() {
                            let _ = self
                                .docker
                                .stop_container(id, None::<StopContainerOptions>)
                                .await;
                            let _ = self
                                .docker
                                .remove_container(id, None::<RemoveContainerOptions>)
                                .await;
                            // Retry creation
                            self.docker
                                .create_container(options, config)
                                .await
                                .map_err(|e| {
                                    ExecutionError::DockerError(
                                        DockerError::ContainerCreationFailed(format!(
                                            "Failed to create container after cleanup: {}",
                                            e
                                        )),
                                    )
                                })?
                        } else {
                            return Err(ExecutionError::DockerError(
                                DockerError::ContainerCreationFailed(format!(
                                    "Failed to create container: {}",
                                    e
                                )),
                            ));
                        }
                    } else {
                        return Err(ExecutionError::DockerError(
                            DockerError::ContainerCreationFailed(format!(
                                "Failed to create container: {}",
                                e
                            )),
                        ));
                    }
                } else {
                    return Err(ExecutionError::DockerError(
                        DockerError::ContainerCreationFailed(format!(
                            "Failed to create container: {}",
                            e
                        )),
                    ));
                }
            }
        };

        let container_id = result.id.clone();
        info!("[EXEC] Container created successfully with ID: {}", container_id);
        Ok(container_id)
    }

    fn get_steps_by_type(
        &self,
        steps: &indexmap::IndexMap<String, Step>,
        step_type: StepType,
    ) -> Vec<(String, Step)> {
        steps
            .iter()
            .filter(|(_, step)| step.step_type == step_type)
            .map(|(name, step)| (name.clone(), step.clone()))
            .collect()
    }

    async fn execute_step(
        &self,
        container_id: &str,
        name: &str,
        step: &Step,
        job: &JobContext,
    ) -> Result<StepResult, ExecutionError> {
        let start_time = Utc::now();
        info!("[EXEC] Preparing to execute step '{}' in container {}", name, container_id);

        // Prepare execution command
        let script = self.prepare_script(step)?;
        info!("[EXEC] Step '{}' script (first 200 chars): {}", name, script.chars().take(200).collect::<String>());
        let exec_config = self.create_exec_config(step, &script, job)?;
        
        if let Some(ref cmd) = exec_config.cmd {
            info!("[EXEC] Step '{}' Docker exec command: {:?}", name, cmd);
        }
        if let Some(ref env) = exec_config.env {
            info!("[EXEC] Step '{}' environment variables: {:?}", name, env);
        }
        
        // Determine the working directory for this step
        // Always use /workspace/{job_id} since we removed working_dir from container config
        let working_dir = format!("/workspace/{}", job.job_id);
        
        info!("[EXEC] Step '{}' working directory: {}", name, working_dir);
        
        // Diagnostic: Check what's in /workspace/ and the job directory
        // This helps debug volume mount issues
        // NOTE: Don't set working_dir in exec options - it will fail if directory doesn't exist
        info!("[EXEC] Running diagnostic check: listing /workspace/ contents and job directory");
        // First check if directory exists and has files, if not, check if we need to copy from host
        let diag_cmd = format!("echo '=== Contents of /workspace/ ===' && ls -la /workspace/ && echo '' && echo '=== Checking job directory: {} ===' && if [ -d {} ]; then echo 'Directory exists'; cd {} && pwd && echo 'Files in directory:' && ls -la; else echo 'Directory does NOT exist'; echo 'Checking if we need to create it from host workspace...'; fi", working_dir, working_dir, working_dir);
        let diag_config = bollard::exec::CreateExecOptions {
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            cmd: Some(vec!["sh".to_string(), "-c".to_string(), diag_cmd]),
            working_dir: None, // Don't set working_dir - let the script handle it
            ..Default::default()
        };
        
        if let Ok(diag_exec) = self.docker.create_exec(container_id, diag_config).await {
            use bollard::exec::StartExecResults;
            if let Ok(StartExecResults::Attached { mut output, .. }) = self.docker.start_exec(&diag_exec.id, None::<bollard::exec::StartExecOptions>).await {
                let mut diag_output = Vec::new();
                while let Some(msg) = output.next().await {
                    match msg {
                        Ok(bollard::container::LogOutput::StdOut { message }) => {
                            diag_output.extend_from_slice(&message);
                        }
                        Ok(bollard::container::LogOutput::StdErr { message }) => {
                            diag_output.extend_from_slice(&message);
                        }
                        _ => {}
                    }
                }
                if !diag_output.is_empty() {
                    let output_str = String::from_utf8_lossy(&diag_output);
                    info!("[EXEC] Diagnostic: Contents of {}:\n{}", working_dir, output_str);
                } else {
                    warn!("[EXEC] Diagnostic: No output received from directory listing");
                }
            } else {
                warn!("[EXEC] Diagnostic: Failed to start exec for directory listing");
            }
        } else {
            warn!("[EXEC] Diagnostic: Failed to create exec for directory listing");
        }

        // Create exec instance
        info!("[EXEC] Creating Docker exec instance for step '{}'", name);
        let exec = self
            .docker
            .create_exec(container_id, exec_config)
            .await
            .map_err(|e| {
                warn!("[EXEC] Failed to create exec for step '{}': {}", name, e);
                ExecutionError::DockerError(DockerError::ContainerExecutionFailed(format!(
                    "Failed to create exec: {}",
                    e
                )))
            })?;
        info!("[EXEC] Docker exec instance created with ID: {}", exec.id);

        // Start execution with streaming
        info!("[EXEC] Starting Docker exec for step '{}'", name);
        use bollard::exec::StartExecResults;
        let exec_result: StartExecResults = self
            .docker
            .start_exec(
                &exec.id,
                Some(StartExecOptions {
                    detach: false,
                    tty: false,
                    output_capacity: Some(1024 * 1024), // 1MB buffer
                }),
            )
            .await
            .map_err(|e| {
                warn!("[EXEC] Failed to start exec for step '{}': {}", name, e);
                ExecutionError::DockerError(DockerError::ContainerExecutionFailed(format!(
                    "Failed to start exec: {}",
                    e
                )))
            })?;
        info!("[EXEC] Docker exec started for step '{}', streaming output...", name);

        // Stream logs
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut line_count = 0;

        match exec_result {
            StartExecResults::Attached { mut output, .. } => {
                while let Some(msg) = output.next().await {
                    match msg {
                        Ok(bollard::container::LogOutput::StdOut { message }) => {
                            stdout.extend_from_slice(&message);
                            line_count += 1;
                            if line_count % 100 == 0 {
                                info!("[EXEC] Step '{}' stdout: received {} lines so far", name, line_count);
                            }
                            if let Some(ref streamer) = self.log_streamer {
                                streamer.send(job.job_id, job.run_id, Some(name.to_string()), crate::models::types::LogLevel::Info, &message);
                            }
                        }
                        Ok(bollard::container::LogOutput::StdErr { message }) => {
                            stderr.extend_from_slice(&message);
                            warn!("[EXEC] Step '{}' stderr output: {}", name, String::from_utf8_lossy(&message));
                            if let Some(ref streamer) = self.log_streamer {
                                streamer.send(job.job_id, job.run_id, Some(name.to_string()), crate::models::types::LogLevel::Error, &message);
                            }
                        }
                        Ok(bollard::container::LogOutput::StdIn { message }) => {
                            // Ignore stdin messages
                            let _ = message;
                        }
                        Ok(bollard::container::LogOutput::Console { message }) => {
                            stdout.extend_from_slice(&message);
                        }
                        Err(e) => {
                            warn!("[EXEC] Error reading exec output for step '{}': {}", name, e);
                            break;
                        }
                    }
                }
                info!("[EXEC] Step '{}' output streaming completed, total lines: {}", name, line_count);
            }
            StartExecResults::Detached => {
                return Err(ExecutionError::DockerError(
                    DockerError::ContainerExecutionFailed(
                        "Exec started in detached mode".to_string(),
                    ),
                ));
            }
        }

        // Get exit code
        info!("[EXEC] Inspecting exec result for step '{}'", name);
        let inspect = self.docker.inspect_exec(&exec.id).await.map_err(|e| {
            warn!("[EXEC] Failed to inspect exec for step '{}': {}", name, e);
            ExecutionError::DockerError(DockerError::ContainerExecutionFailed(format!(
                "Failed to inspect exec: {}",
                e
            )))
        })?;

        let exit_code = inspect.exit_code.unwrap_or(-1) as i32;
        info!("[EXEC] Step '{}' exec completed with exit code: {}", name, exit_code);

        Ok(StepResult {
            name: name.to_string(),
            step_type: step.step_type,
            exit_code,
            started_at: start_time,
            finished_at: Utc::now(),
            stdout: String::from_utf8_lossy(&stdout).to_string(),
            stderr: String::from_utf8_lossy(&stderr).to_string(),
        })
    }

    fn prepare_script(&self, step: &Step) -> Result<String, ExecutionError> {
        let script = step.scripts.join("\n");
        Ok(format!("set -e\n{}", script))
    }

    fn create_exec_config(
        &self,
        step: &Step,
        script: &str,
        job: &JobContext,
    ) -> Result<CreateExecOptions<String>, ExecutionError> {
        let shell_cmd = match step.shell {
            crate::models::types::Shell::Bash => "bash",
            crate::models::types::Shell::Sh => "sh",
            crate::models::types::Shell::Python => "python",
            crate::models::types::Shell::Node => "node",
        };

        // Always cd into the workspace directory first
        // Use step's working_directory if specified, otherwise use /workspace/{job_id}
        let workspace_dir = step.working_directory.as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("/workspace/{}", job.job_id));
        
        // Prepend cd command to ensure we're in the right directory
        // Create directory if it doesn't exist (mkdir -p is safe, won't fail if exists)
        // IMPORTANT: The workspace_dir should already exist with files from cloning,
        // but if it doesn't exist or is empty, we need to handle it gracefully
        // The diagnostic will show us what's actually in the volume
        let script_with_cd = format!("if [ ! -d {} ] || [ -z \"$(ls -A {})\" ]; then echo 'WARNING: Workspace directory {} does not exist or is empty!'; mkdir -p {}; fi && cd {} && {}", workspace_dir, workspace_dir, workspace_dir, workspace_dir, workspace_dir, script);
        
        let cmd = vec![shell_cmd.to_string(), "-c".to_string(), script_with_cd];

        let mut env_vars = Vec::new();

        // Add step-specific env vars
        for (key, value) in &step.envs {
            env_vars.push(format!("{}={}", key, value));
        }

        Ok(CreateExecOptions {
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            attach_stdin: Some(false),
            cmd: Some(cmd),
            env: Some(env_vars),
            // Don't set working_dir here - we're already cd'ing in the script
            // Setting it here causes Docker to fail if the directory doesn't exist
            working_dir: None,
            privileged: Some(false),
            user: None,
            detach_keys: None,
            tty: Some(false),
        })
    }
}
