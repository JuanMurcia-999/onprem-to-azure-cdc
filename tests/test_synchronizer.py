import pytest
import os
from unittest.mock import MagicMock
from synchronizer import get_data_columns, apply_insert, apply_update, apply_delete

SCHEMA = os.getenv('DB_SCHEMA', 'dbo')

RECORD_CDC = {
    '__$start_lsn'  : b'\x00\x00\x02\xae\x00\x01\xa4\x18\x00\x04',
    '__$seqval'     : b'\x00\x00\x02\xae\x00\x01\xa4\x18\x00\x03',
    '__$operation'  : 2,
    '__$update_mask': b'\x03',
    'id_section'    : 10,
    'sect_name'     : 'Test'
}

class TestGetDataColumns:
    def test_elimina_columnas_control(self):
        resultado = get_data_columns(RECORD_CDC)
        for key in resultado.keys():
            assert not key.startswith('__$')

    def test_conserva_columnas_reales(self):
        resultado = get_data_columns(RECORD_CDC)
        assert 'id_section' in resultado
        assert 'sect_name'  in resultado

    def test_valores_correctos(self):
        resultado = get_data_columns(RECORD_CDC)
        assert resultado['id_section'] == 10
        assert resultado['sect_name']  == 'Test'

class TestApplyInsert:
    def test_construye_sql_correcto(self):
        cursor  = MagicMock()
        columns = {'id_section': 10, 'sect_name': 'Test'}
        apply_insert(cursor, 'sections', columns)
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert f'INSERT INTO {SCHEMA}.sections' in sql
        assert 'id_section'                     in sql
        assert 'sect_name'                      in sql

    def test_verifica_existencia_antes_de_insertar(self):
        cursor  = MagicMock()
        columns = {'id_section': 10, 'sect_name': 'Test'}
        apply_insert(cursor, 'sections', columns)
        sql = cursor.execute.call_args[0][0]
        assert 'IF NOT EXISTS' in sql

class TestApplyUpdate:
    def test_construye_sql_correcto(self):
        cursor  = MagicMock()
        columns = {'id_section': 10, 'sect_name': 'Modificado'}
        apply_update(cursor, 'sections', columns)
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert f'UPDATE {SCHEMA}.sections' in sql
        assert 'SET sect_name'             in sql
        assert 'WHERE id_section'          in sql

    def test_usa_primera_columna_como_pk(self):
        cursor  = MagicMock()
        columns = {'id_section': 10, 'sect_name': 'Modificado'}
        apply_update(cursor, 'sections', columns)
        sql    = cursor.execute.call_args[0][0]
        values = cursor.execute.call_args[0][1]
        assert 'WHERE id_section' in sql
        assert values[-1] == 10

class TestApplyDelete:
    def test_construye_sql_correcto(self):
        cursor  = MagicMock()
        columns = {'id_section': 10, 'sect_name': 'Test'}
        apply_delete(cursor, 'sections', columns)
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert f'DELETE FROM {SCHEMA}.sections' in sql
        assert 'WHERE id_section'               in sql

    def test_usa_primera_columna_como_pk(self):
        cursor  = MagicMock()
        columns = {'id_section': 10, 'sect_name': 'Test'}
        apply_delete(cursor, 'sections', columns)
        values = cursor.execute.call_args[0][1]
        assert values[0] == 10