import os
from logger import get_logger
from watermark_manager import WatermarkManager
from cdc_utils import get_cdc_tables, table_exists_in_azure
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('/home/duplicate/.env')

log    = get_logger('synchronizer')
SCHEMA = os.getenv('DB_SCHEMA', 'dbo')

OPERATIONS = {
    1: 'DELETE',
    2: 'INSERT',
    3: 'BEFORE_UPDATE',
    4: 'AFTER_UPDATE'
}

def get_data_columns(record: dict) -> dict:
    return {k: v for k, v in record.items() if not k.startswith('__$')}

def apply_insert(cursor, tabla: str, columns: dict):
    fields       = ', '.join(columns.keys())
    placeholders = ', '.join(['%s'] * len(columns))
    cursor.execute(f"""
        IF NOT EXISTS (SELECT 1 FROM {SCHEMA}.{tabla} WHERE {list(columns.keys())[0]} = %s)
        INSERT INTO {SCHEMA}.{tabla} ({fields}) VALUES ({placeholders})
    """, [list(columns.values())[0]] + list(columns.values()))

def apply_update(cursor, tabla: str, columns: dict):
    pk     = list(columns.keys())[0]
    sets   = ', '.join([f"{k} = %s" for k in columns.keys() if k != pk])
    values = [v for k, v in columns.items() if k != pk]
    values.append(columns[pk])
    cursor.execute(f"UPDATE {SCHEMA}.{tabla} SET {sets} WHERE {pk} = %s", values)

def apply_delete(cursor, tabla: str, columns: dict):
    pk = list(columns.keys())[0]
    cursor.execute(f"DELETE FROM {SCHEMA}.{tabla} WHERE {pk} = %s", [columns[pk]])

def sincronizar(db):
    inicio = datetime.now()
    log.info("="*60)
    log.info(f"INICIO DE CICLO | {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"ESQUEMA         | {SCHEMA}")
    log.info("="*60)

    watermark    = WatermarkManager()
    cursor_local = db.local.cursor(as_dict=True)
    cursor_azure = db.azure.cursor(as_dict=True)

    tablas = get_cdc_tables(cursor_local)

    resultados = {
        'exitosas'   : [],
        'sin_cambios': [],
        'fallidas'   : [],
        'no_en_azure': []
    }

    for tabla in tablas:
        source_table     = tabla['source_table']
        capture_instance = tabla['capture_instance']
        t_inicio         = datetime.now()

        log.info(f"{'─'*40}")
        log.info(f"{source_table} | Iniciando sincronización")

        try:
            if not table_exists_in_azure(cursor_azure, source_table):
                resultados['no_en_azure'].append(source_table)
                continue

            cursor_local.execute("SELECT sys.fn_cdc_get_max_lsn() AS max_lsn")
            max_lsn  = cursor_local.fetchone()['max_lsn']
            last_lsn = watermark.get_last_lsn(capture_instance)

            if last_lsn is None:
                last_lsn = watermark.get_min_lsn(cursor_local, capture_instance)

            if last_lsn >= max_lsn:
                log.info(f"{source_table} | Sin cambios nuevos")
                resultados['sin_cambios'].append(source_table)
                continue

            cursor_local.execute(f"""
                SELECT * FROM cdc.fn_cdc_get_all_changes_{capture_instance}(%s, %s, 'all')
                ORDER BY __$start_lsn
            """, (last_lsn, max_lsn))

            cambios = cursor_local.fetchall()
            log.info(f"{source_table} | {len(cambios)} cambio(s) encontrado(s)")

            for c in cambios:
                op      = c['__$operation']
                columns = get_data_columns(c)
                log.info(f"{source_table} | {OPERATIONS.get(op)} | {columns}")

                if op == 2:
                    apply_insert(cursor_azure, source_table, columns)
                elif op == 4:
                    apply_update(cursor_azure, source_table, columns)
                elif op == 1:
                    apply_delete(cursor_azure, source_table, columns)

            db.azure.commit()
            watermark.save_last_lsn(capture_instance, max_lsn)

            duracion = (datetime.now() - t_inicio).total_seconds()
            log.info(f"{source_table} | ✅ Completada en {duracion:.2f}s")
            resultados['exitosas'].append(source_table)

        except Exception as e:
            log.error(f"{source_table} | ❌ Error: {e}")
            db.azure.rollback()
            resultados['fallidas'].append(source_table)

    duracion_total = (datetime.now() - inicio).total_seconds()
    log.info("="*60)
    log.info(f"RESUMEN DEL CICLO | Duración total: {duracion_total:.2f}s")
    log.info(f"  ✅ Exitosas    : {resultados['exitosas']}")
    log.info(f"  ⏭️  Sin cambios : {resultados['sin_cambios']}")
    log.info(f"  ❌ Fallidas    : {resultados['fallidas']}")
    log.info(f"  ⚠️  No en Azure : {resultados['no_en_azure']}")
    log.info("="*60)