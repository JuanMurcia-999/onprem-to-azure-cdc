import pytest
from unittest.mock import MagicMock
from cdc_utils import get_cdc_tables, table_exists_in_azure

class TestGetCdcTables:
    def test_retorna_tablas_activas(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {'source_schema': 'dbo', 'source_table': 'sections',  'capture_instance': 'dbo_sections'},
            {'source_schema': 'dbo', 'source_table': 'families', 'capture_instance': 'dbo_families'}
        ]
        resultado = get_cdc_tables(cursor)
        assert len(resultado) == 2
        assert resultado[0]['source_table'] == 'sections'
        assert resultado[1]['source_table'] == 'families'

    def test_retorna_lista_vacia_sin_tablas_cdc(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        resultado = get_cdc_tables(cursor)
        assert resultado == []

class TestTableExistsInAzure:
    def test_tabla_existe(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = {'total': 1}
        assert table_exists_in_azure(cursor, 'sections') is True

    def test_tabla_no_existe(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = {'total': 0}
        assert table_exists_in_azure(cursor, 'tabla_inexistente') is False