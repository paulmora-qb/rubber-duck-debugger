import subprocess
from pathlib import Path

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "run_daily_ingest.sh"
PIPELINES = ["data_ingestion"]


def test_dry_run_exits_zero():
    result = subprocess.run(
        ["/bin/bash", str(SCRIPT), "--dry-run"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_dry_run_contains_all_pipelines():
    result = subprocess.run(
        ["/bin/bash", str(SCRIPT), "--dry-run"],
        capture_output=True,
        text=True,
        check=False,
    )
    for pipeline in PIPELINES:
        assert pipeline in result.stdout, (
            f"Missing pipeline in dry-run output: {pipeline}"
        )


def test_dry_run_pipeline_order():
    result = subprocess.run(
        ["/bin/bash", str(SCRIPT), "--dry-run"],
        capture_output=True,
        text=True,
        check=False,
    )
    positions = [result.stdout.index(p) for p in PIPELINES]
    assert positions == sorted(positions), "Pipelines not printed in expected order"
