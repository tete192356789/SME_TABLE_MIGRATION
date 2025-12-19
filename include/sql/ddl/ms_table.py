QUERY = """

CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
	ProductID INT PRIMARY KEY ,
	created_date datetime NULL default current_timestamp,
	updated_date datetime NULL default current_timestamp
);
"""
