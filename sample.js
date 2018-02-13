module.exports =  {
    "schema": "dbTemplate",
    "tables": [
        {
            "table_name": "tbl_Company",
            "columns": [
                {
                    "column_name": "name",
                    "unique": true,
                    "type": "string"
                }
            ]
        },
        {
            "table_name": "tbl_User",
            "columns": [
                {
                    "column_name": "username",
                    "type": "string"
                },
                {
                    "column_name": "company_id",
                    "type": "integer",
                    "foreign_key": true,
                    "unsigned": true,
                    "reference_table": "tbl_Company",
                    "reference_column": "id",
                    "on_update": 'CASCADE',
                    "on_delete": 'RESTRICT'
                }
            ],
            "indices": ['username']
        }
    ]
}