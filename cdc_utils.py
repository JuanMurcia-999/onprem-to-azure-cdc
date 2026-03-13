from logger import get_logger

log = get_logger('cdc_utils')

def get_cdc_tables(cursor) -> list:
    cursor.execute("""
        SELECT 
            object_schema_name(source_object_id) AS source_schema,
            object_name(source_object_id)        AS source_table,
            capture_instance
        FROM cdc.change_tables
    """)
    tables = cursor.fetchall()
    log.info(f"{len(tables)} tabla(s) activas en CDC: {[t['source_table'] for t in tables]}")
    return tables

def table_exists_in_azure(cursor, tabla: str) -> bool:
    cursor.execute("""
        SELECT COUNT(*) AS total
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME = %s
    """, (tabla,))
    exists = cursor.fetchone()['total'] > 0
    if not exists:
        log.warning(f"{tabla} | No existe en Azure, se omite la sincronización")
    return exists