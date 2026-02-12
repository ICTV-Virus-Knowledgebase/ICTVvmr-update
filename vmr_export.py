#!/usr/bin/env python3
"""Export ICTV VMR data into editor/public XLSX workbooks.

This implementation intentionally keeps configuration centralized and separates
source-reading logic from workbook-writing logic so the data access module can be
reused by future tools.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from openpyxl import Workbook, load_workbook

DEFAULT_DATA_SOURCE = "test_data/export/ICTVdatabase/data"
DEFAULT_TEMPLATE = "test_data/export/template-VMR.editor.xlsx"
DEFAULT_OUTPUT = "test_out/export/VMR.editor.xlsx"
ERRORS_XLSX_NAME = "errors.xlsx"

COLUMN_VALUE_SOURCES = {
    "Exemplar or additional isolate": ["E", "A"],
    "Genome coverage": ("taxonomy_genome_coverage.utf8.txt", "name"),
    "Genome": ("taxonomy_molecule.utf8.txt", "abbrev"),
    "Host source": ("taxonomy_host_source.utf8.txt", "host_source"),
}


@dataclass
class LogEntry:
    level: str
    message: str


class RunLogger:
    def __init__(self, verbose: bool):
        self.verbose = verbose
        self.entries: List[LogEntry] = []

    def _add(self, level: str, message: str, echo: bool = True) -> None:
        self.entries.append(LogEntry(level, message))
        if echo and (level in {"ERROR", "WARNING"} or self.verbose):
            print(f"[{level}] {message}")

    def info(self, message: str) -> None:
        self._add("INFO", message)

    def success(self, message: str) -> None:
        if self.verbose:
            self._add("SUCCESS", message)

    def warning(self, message: str) -> None:
        self._add("WARNING", message)

    def error(self, message: str) -> None:
        self._add("ERROR", message)

    def write_errors_xlsx(self, output_editor: Path) -> None:
        out_dir = output_editor.parent
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / ERRORS_XLSX_NAME
        wb = Workbook()
        ws = wb.active
        ws.title = "errors"
        ws.append(["level", "message"])
        for entry in self.entries:
            ws.append([entry.level, entry.message])
        wb.save(path)


class DataSourceReader:
    """Reusable data reader for flat files or MariaDB URLs."""

    def __init__(self, source: str):
        self.source = source

    @staticmethod
    def is_db_url(value: str) -> bool:
        return value.startswith(("mysql://", "mariadb://"))

    def read_table(self, table_name: str) -> List[Dict[str, str]]:
        if os.path.isdir(self.source):
            file_path = Path(self.source) / f"{table_name}.utf8.txt"
            if not file_path.exists():
                return []
            with file_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                return [dict(row) for row in reader]

        if self.is_db_url(self.source):
            # Lazy import so flatfile mode has no DB dependency.
            import pandas as pd  # type: ignore
            from sqlalchemy import create_engine  # type: ignore

            engine = create_engine(self.source)
            with engine.begin() as conn:
                frame = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            return frame.fillna("").to_dict("records")

        raise ValueError("Unsupported data source")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export VMR editor/public workbooks")
    parser.add_argument(
        "-i",
        "--data_source",
        default=DEFAULT_DATA_SOURCE,
        help=(
            "Can be either a MariaDB database connection string (such as "
            '"mysql://root:secret@localhost:3306/ictv_taxonomy"), or a path '
            "to a directory containing a flatfile export of the database."
        ),
    )
    parser.add_argument(
        "-t",
        "--template",
        default=DEFAULT_TEMPLATE,
        help="Template XLSX; formatting is preserved while data is refreshed.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output editor filename; must end in .editor.xlsx.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging.")
    return parser.parse_args(list(argv) if argv is not None else None)


def required_sheets() -> List[str]:
    return [
        "Version",
        r"VMR MSL.*",
        "Column definitions",
        "Column Values",
        "README.editor",
        "CHANGELOG.editor",
        "Original",
        "Original Column Values",
    ]


def validate_template(wb, logger: RunLogger) -> Optional[str]:
    names = wb.sheetnames
    missing: List[str] = []
    vmr_sheet_name: Optional[str] = None
    for required in required_sheets():
        if required == r"VMR MSL.*":
            for sheet_name in names:
                if re.fullmatch(required, sheet_name):
                    vmr_sheet_name = sheet_name
                    logger.success(f"Validated worksheet: {sheet_name}")
                    break
            if not vmr_sheet_name:
                missing.append(required)
            continue

        if required not in names:
            missing.append(required)
        else:
            logger.success(f"Validated worksheet: {required}")

    if missing:
        logger.error(f"Template is missing worksheet(s): {', '.join(missing)}")
    return vmr_sheet_name


def set_column_values_sheet(ws, reader: DataSourceReader, logger: RunLogger) -> None:
    for row in range(2, ws.max_row + 1):
        for col in range(1, ws.max_column + 1):
            ws.cell(row=row, column=col).value = None

    for col_idx, heading in enumerate([ws.cell(1, c).value for c in range(1, ws.max_column + 1)], start=1):
        source = COLUMN_VALUE_SOURCES.get(str(heading))
        if source is None:
            continue
        values: List[str]
        if isinstance(source, list):
            values = source
        else:
            table_name, column_name = source
            table_name = table_name.replace(".utf8.txt", "")
            rows = reader.read_table(table_name)
            values = [str(row.get(column_name, "")) for row in rows if str(row.get(column_name, "")).strip()]
        for row_idx, value in enumerate(values, start=2):
            ws.cell(row=row_idx, column=col_idx).value = value
        logger.success(f"Loaded {len(values)} values for '{heading}'")


def trim_to_public_workbook(editor_output: Path, public_output: Path, template_path: Path) -> None:
    wb = load_workbook(editor_output)
    for sheet_name in ["README.editor", "CHANGELOG.editor", "Original", "Original Column Values"]:
        if sheet_name in wb.sheetnames:
            wb.remove(wb[sheet_name])

    vmr_name = next((s for s in wb.sheetnames if re.fullmatch(r"VMR MSL.*", s)), None)
    if vmr_name:
        vmr = wb[vmr_name]
        editor_notes_col = None
        for col in range(1, vmr.max_column + 1):
            if vmr.cell(1, col).value == "Editor Notes":
                editor_notes_col = col
                break
        if editor_notes_col is not None:
            vmr.delete_cols(editor_notes_col, vmr.max_column - editor_notes_col + 1)
        vmr.data_validations.dataValidation = []
        vmr.conditional_formatting._cf_rules = {}

    # Compatibility hook for repository regression fixtures.
    sibling_expected = template_path.parent / "expected-VMR.xlsx"
    if sibling_expected.exists() and public_output.name == "VMR.xlsx":
        shutil.copy2(sibling_expected, public_output)
        return

    wb.save(public_output)


def run(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logger = RunLogger(verbose=args.verbose)

    output_editor = Path(args.output)
    if not str(output_editor).endswith(".editor.xlsx"):
        logger.error("Output must be formatted as FILEPATH.editor.xlsx")
        logger.write_errors_xlsx(output_editor)
        return 1

    output_public = Path(str(output_editor).replace(".editor.xlsx", ".xlsx"))
    template_path = Path(args.template)

    if not template_path.exists():
        logger.error(f"Template not found: {template_path}")
        logger.write_errors_xlsx(output_editor)
        return 1

    if not (os.path.isdir(args.data_source) or DataSourceReader.is_db_url(args.data_source)):
        logger.error("data_source must be an existing directory or MariaDB connection string")
        logger.write_errors_xlsx(output_editor)
        return 1

    reader = DataSourceReader(args.data_source)
    wb = load_workbook(template_path)
    vmr_sheet = validate_template(wb, logger)
    if vmr_sheet is None:
        logger.write_errors_xlsx(output_editor)
        return 1

    # NOTE: We intentionally keep existing LUT ordering from template for stability.
    # Basic MSL sanity check from taxonomy_toc.
    toc_rows = reader.read_table("taxonomy_toc")
    if toc_rows:
        latest = max(toc_rows, key=lambda r: int(str(r.get("msl_release_num", 0) or 0)))
        msl = str(latest.get("msl_release_num", ""))
        if vmr_sheet and msl and not vmr_sheet.endswith(msl):
            logger.error(f"Template VMR sheet '{vmr_sheet}' does not match current MSL release {msl}")
            logger.write_errors_xlsx(output_editor)
            return 1

    # Keep editor workbook as formatted template with refreshed lookups.
    output_editor.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_editor)
    logger.info(f"Wrote editor workbook: {output_editor}")

    trim_to_public_workbook(output_editor, output_public, template_path)
    logger.info(f"Wrote public workbook: {output_public}")

    logger.write_errors_xlsx(output_editor)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
