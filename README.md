# SonicWave Ingestion Pipeline

PySpark ingestion package for SonicWave `plays` and `users`.

## Structure

- `seed/` generates raw JSON snapshots under `data/source/<table>/<snapshot_date>/`.
- `src/sonicwave_ingestion/bronze/` lands permissive Bronze parquet with provenance.
- `src/sonicwave_ingestion/silver/` applies typing, validation, deduplication and SCD2.
- `src/sonicwave_ingestion/scripts/` contains thin CLI entry-points.
- `tests/` covers transforms and pipeline behaviour.

## Design Choices

Source data is stored as JSON because it is permissive and can preserve dirty input.
Bronze and Silver are stored as partitioned Parquet because downstream reads should be
typed, columnar and cheap to scan.

Current loading model:
- Bronze is loaded per `snapshot_date`.
- Bronze writes are partition-overwrite by `snapshot_date`, so rerunning the same drop replaces only that partition.
- Bronze entry-points accept `--bronze-path` (default `./data/bronze`), while Silver entry-points accept `--silver-path` (default `./data/silver`).
- Silver reads the Bronze partition selected by `--snapshot-date`.
- `plays` rewrites the complete processed `snapshot_date` partition in Silver. A corrected or re-dropped day therefore retains its existing rows, incorporates new `play_id` values and preserves their surrogate keys.
- `plays_errors` is also a complete per-day result: a corrected re-drop with no remaining errors clears that day's error partition.
- `plays` keeps `snapshot_date` as the physical arrival partition and derives `event_date` from `played_at` for event-time analysis. `late_arriving_data` is true when `created_at` falls on a later date than `played_at`.
- `users` is historised as SCD2 with `valid_from`, `valid_to` and `is_current`, ordered by `coalesce(updated_at, created_at)`.

## Local Run

Install dependencies:

```bash
uv sync
```

Generate source snapshots:

```bash
uv run python seed/generate_seed.py --output ./data/source
```

Run Bronze:

```bash
uv run sonicwave-run-bronze-plays \
  --source ./data/source/plays \
  --snapshot-date 2026-03-01

uv run sonicwave-run-bronze-users \
  --source ./data/source/users \
  --snapshot-date 2026-03-01
```

Run Silver:

```bash
uv run sonicwave-run-silver-plays \
  --source ./data/bronze/plays \
  --snapshot-date 2026-03-01

uv run sonicwave-run-silver-users \
  --source ./data/bronze/users \
  --snapshot-date 2026-03-01
```

Inspect Silver `plays`:

```bash
uv run python src/sonicwave_ingestion/views/view_plays.py \
  --path ./data/silver/plays
```

## Data Quality

Silver applies:
- explicit `StructType` casting with `try_cast`
- validation for required fields and numeric ranges
- window-based deduplication with `row_number()`
- quarantine tables for cast, validation and deduplication errors

Bronze also quarantines structurally corrupt JSON rows into `data/bronze/<table>_errors`.

## Tooling

Run checks locally:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest -q
```

GitHub Actions:
- CI: `.github/workflows/ci.yml`
- Release: `.github/workflows/release.yml`
