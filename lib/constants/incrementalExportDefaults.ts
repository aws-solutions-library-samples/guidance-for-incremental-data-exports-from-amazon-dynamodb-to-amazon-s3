export class IncrementalExportDefaults {
    public static DEFAULT_INCREMENTAL_EXPORT_WINDOW_SIZE_IN_MINUTES = 15 as number;
    public static WAIT_TIME_TO_CHECK_EXPORT_STATUS_IN_SECONDS = 10 as number;
    public static DATA_EXPORT_FORMAT = 'DYNAMODB_JSON';
    public static AWS_API_INVOCATION_TIMEOUT_IN_SECONDS = 10 as number;
    
    public static SDK_EXCEPTION_RETRY_ATTEMPTS = 5 as number;
    public static SDK_EXCEPTION_INTERVAL_DURATION_SECONDS = 20 as number;
    public static SDK_EXCEPTION_BACKOFF_RATE = 1 as number;
}
