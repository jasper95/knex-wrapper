module.exports =  {
    "schema": "dbTemplate",
    "tables": [
        {
            "table_name": "tbl_Company",
            "columns": [
                {
                    "column_name": "name",
                    "type": "string",
                    "unique": true
                },
                {
                    "column_name": "legal_name",
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
                    "type": "uuid",
                    "foreign_key": true,
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