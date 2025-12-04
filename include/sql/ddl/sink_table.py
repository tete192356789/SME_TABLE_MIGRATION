QUERY = """CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
	id serial4 NOT NULL,
	created_date timestamp NULL,
	updated_date timestamp NULL,
	CONSTRAINT sink_table_pkey PRIMARY KEY (id)
);"""
