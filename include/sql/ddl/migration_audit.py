QUERY = """
CREATE TABLE IF NOT EXISTS {LOG_TABLE_NAME} (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255),
    run_id VARCHAR(255),
    status VARCHAR(255),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    source_table VARCHAR(255),
    sink_table VARCHAR(255),
    source_count BIGINT,
    updated_count BIGINT,
    inserted_count BIGINT,
    sink_count_before BIGINT,
    sink_count_after BIGINT,
    duration_seconds INT,
    err_task_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
