"""
Utilidades para leer archivos CSV o Excel usando Polars.

Este módulo proporciona una función principal (`read_tabular_file`)
que detecta el formato a partir de la extensión del archivo y aplica
la rutina de carga adecuada.
"""

from __future__ import annotations

from pathlib import Path
from typing import Mapping, Sequence

import polars as pl

_CSV_EXTENSIONS = {".csv", ".tsv", ".txt"}
_EXCEL_EXTENSIONS = {".xls", ".xlsx"}


class UnsupportedFormatError(ValueError):
    """Se lanza cuando la extensión del archivo no es compatible."""


def _normalize_path(file_path: str | Path) -> Path:
    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {path}")
    return path


def _infer_format(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in _CSV_EXTENSIONS:
        return "csv"
    if ext in _EXCEL_EXTENSIONS:
        return "excel"
    supported = ", ".join(sorted(_CSV_EXTENSIONS | _EXCEL_EXTENSIONS))
    raise UnsupportedFormatError(
        f"Extensión '{ext}' no soportada. Usa alguna de: {supported}"
    )


def read_tabular_file(
    file_path: str | Path,
    *,
    columns: Sequence[str] | None = None,
    column_aliases: Mapping[str, str] | None = None,
    sheet: str | int | None = None,
    separator: str | None = None,
    has_header: bool = True,
    infer_schema_length: int | str = 1_000,
    try_parse_dates: bool = True,
) -> pl.DataFrame:
    """
    Lee un archivo CSV o Excel y devuelve un `pl.DataFrame`.

    Parámetros
    ----------
    file_path:
        Ruta al archivo CSV/Excel.
    columns:
        Lista de columnas a conservar (se aplica después de la carga).
    column_aliases:
        Diccionario opcional para renombrar columnas tras la lectura.
    sheet:
        Nombre o índice de la hoja (solo Excel). Por defecto, la primera.
    separator:
        Separador para CSV. Si no se indica, se infiere por extensión
        (`','` para `.csv` y `'\t'` para `.tsv`).
    has_header:
        Indica si el archivo CSV posee fila de encabezados.
    infer_schema_length:
        Número de filas que Polars usa para inferir tipos en CSV.
    try_parse_dates:
        Si es `True`, intenta convertir columnas con formato fecha.
    """

    path = _normalize_path(file_path)
    file_format = _infer_format(path)

    if file_format == "excel":
        df = pl.read_excel(
            path,
            sheet_id=sheet if isinstance(sheet, int) else None,
            sheet_name=sheet if isinstance(sheet, str) else None,
            infer_schema_length=infer_schema_length,
            read_options={"try_parse_dates": try_parse_dates},
        )
    else:
        inferred_sep = "\t" if path.suffix.lower() == ".tsv" else ","
        df = pl.read_csv(
            path,
            has_header=has_header,
            separator=separator or inferred_sep,
            infer_schema_length=infer_schema_length,
            try_parse_dates=try_parse_dates,
        )

    if columns:
        missing = [col for col in columns if col not in df.columns]
        if missing:
            raise KeyError(f"Columnas no encontradas: {missing}")
        df = df.select(columns)

    if column_aliases:
        df = df.rename(column_aliases)

    return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Lectura rápida de archivos tabulares con Polars."
    )
    parser.add_argument("file_path", help="Ruta del archivo .csv/.tsv/.xls/.xlsx")
    parser.add_argument(
        "--sheet",
        help="Nombre o índice de la hoja (solo para Excel).",
    )
    parser.add_argument(
        "--columns",
        nargs="+",
        help="Columnas a conservar (separadas por espacio).",
    )

    args = parser.parse_args()

    df = read_tabular_file(
        args.file_path,
        sheet=int(args.sheet) if args.sheet and args.sheet.isdigit() else args.sheet,
        columns=args.columns,
    )
    print(df.head())
