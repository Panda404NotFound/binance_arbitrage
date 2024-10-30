#[allow(dead_code)]
#[derive(Default, Debug, Clone)]
pub struct ErrorStatus {
    pub handle_shutdown_signal_error: Option<String>,
    pub internet_connection_failures: Option<String>,
    pub function_startup_state: Option<String>,
    pub restart_process_error: Option<String>,
}