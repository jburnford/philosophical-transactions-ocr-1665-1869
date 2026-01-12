#!/usr/bin/env python3
"""
Split OLMoCR JSONL files into individual JSON files per PDF.
Adapted for the Royal Society collection - outputs as jstor-XXXXXX.json
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import re


PDF_SOURCE_KEYS = (
    "Source-File",
    "source_file",
    "source",
    "filename",
    "file_name",
    "path",
    "filepath",
    "pdf",
    "pdf_name",
    "document",
    "document_name",
)


def _safe_parse_metadata(md: Any) -> Optional[Dict[str, Any]]:
    """Return metadata as dict when possible (handles stringified JSON)."""
    if md is None:
        return None
    if isinstance(md, dict):
        return md
    if isinstance(md, str):
        try:
            parsed = json.loads(md)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None


def _extract_source_file(obj: Dict[str, Any]) -> Optional[str]:
    """Extract source PDF path/filename from an object."""
    md = _safe_parse_metadata(obj.get("metadata")) or {}
    # Prefer metadata keys
    for k in PDF_SOURCE_KEYS:
        v = md.get(k)
        if isinstance(v, str) and v:
            return v
    # Fallback to top-level keys
    for k in PDF_SOURCE_KEYS:
        v = obj.get(k)
        if isinstance(v, str) and v:
            return v
    return None


def _extract_jstor_id(source_file: str) -> Optional[str]:
    """Extract JSTOR ID from source file path like /path/to/104588.pdf -> jstor-104588"""
    filename = Path(source_file).stem  # Get filename without extension
    # Match numeric ID
    match = re.match(r'^(\d+)$', filename)
    if match:
        return f"jstor-{match.group(1)}"
    # Already has jstor- prefix
    if filename.startswith('jstor-'):
        return filename
    return None


def parse_jsonl_file(jsonl_file: Path) -> Tuple[Dict[str, List[dict]], List[Tuple[int, str]]]:
    """Parse a JSONL file and group records by JSTOR identifier."""
    grouped: Dict[str, List[dict]] = defaultdict(list)
    issues: List[Tuple[int, str]] = []

    with jsonl_file.open('r', encoding='utf-8') as f:
        for line_no, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                issues.append((line_no, f"JSON decode error: {e}"))
                continue

            source_file = _extract_source_file(obj)
            if not source_file:
                issues.append((line_no, "No source file found"))
                continue

            jstor_id = _extract_jstor_id(source_file)
            if not jstor_id:
                issues.append((line_no, f"Could not extract JSTOR ID from: {source_file}"))
                continue

            grouped[jstor_id].append(obj)

    return dict(grouped), issues


def split_jsonl_files(input_dir: Path, output_dir: Path, dry_run: bool = False):
    """Split JSONL files into individual JSON files."""

    # Find all JSONL files (excluding the metadata file)
    jsonl_files = [f for f in input_dir.glob("*.jsonl")
                   if not f.name.startswith("jstor_metadata")]

    if not jsonl_files:
        print(f"No JSONL files found in {input_dir}")
        return False

    print("=" * 70)
    print("Split OLMoCR JSONL to Individual JSON Files")
    print("=" * 70)
    print(f"Input directory:  {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"JSONL files found: {len(jsonl_files)}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("-" * 70)

    all_grouped = {}
    total_issues = 0
    total_records = 0

    for jsonl_file in sorted(jsonl_files):
        print(f"\nProcessing: {jsonl_file.name}")
        grouped, issues = parse_jsonl_file(jsonl_file)

        records_in_file = sum(len(records) for records in grouped.values())
        total_records += records_in_file
        print(f"  Records: {records_in_file}")
        print(f"  Unique PDFs: {len(grouped)}")

        if issues:
            print(f"  Issues: {len(issues)}")
            for line_no, msg in issues[:3]:
                print(f"    Line {line_no}: {msg}")
            if len(issues) > 3:
                print(f"    ... and {len(issues) - 3} more")

        # Merge results
        for jstor_id, records in grouped.items():
            if jstor_id in all_grouped:
                print(f"  Warning: {jstor_id} already seen, appending records")
                all_grouped[jstor_id].extend(records)
            else:
                all_grouped[jstor_id] = records

        total_issues += len(issues)

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"JSONL files processed: {len(jsonl_files)}")
    print(f"Total records: {total_records}")
    print(f"Unique JSTOR IDs: {len(all_grouped)}")
    print(f"Total issues: {total_issues}")

    if not all_grouped:
        print("\nNo records could be extracted")
        return False

    # Create output directory
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    # Save individual JSON files
    print(f"\n{'Would save' if dry_run else 'Saving'} JSON files:")
    print("-" * 70)

    saved_count = 0
    for jstor_id, records in sorted(all_grouped.items()):
        json_filename = f"{jstor_id}.json"
        json_path = output_dir / json_filename

        # For single record, save the object directly; for multiple, save as array
        if len(records) == 1:
            output_data = records[0]
        else:
            output_data = records

        if not dry_run:
            with json_path.open('w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)

        if saved_count < 10 or saved_count % 100 == 0:
            print(f"  {'[DRY RUN] ' if dry_run else ''}{json_filename}: {len(records)} record(s)")
        saved_count += 1

    print(f"  ... ({saved_count} files total)")

    print("\n" + "=" * 70)
    if dry_run:
        print(f"DRY RUN: Would create {saved_count} JSON files in {output_dir}")
    else:
        print(f"Successfully created {saved_count} JSON files in {output_dir}")
    print("=" * 70)

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Split OLMoCR JSONL files into individual JSON files per PDF"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path(__file__).parent,
        help="Directory containing JSONL files (default: script directory)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for JSON files (default: input_dir/json)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without creating files"
    )

    args = parser.parse_args()

    if args.output_dir is None:
        args.output_dir = args.input_dir / "json"

    if not args.input_dir.exists():
        print(f"Error: Input directory not found: {args.input_dir}")
        return 1

    success = split_jsonl_files(args.input_dir, args.output_dir, args.dry_run)
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
