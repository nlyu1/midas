"""
CTDP Streamlit App Entry Point

Usage:
    streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path /data/atlas/ctdp_xxx

    Or with uv:
    uv run streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path /data/atlas/ctdp_xxx
"""

import argparse
from pathlib import Path
from atlas.ctdp import run_app


def main():
    parser = argparse.ArgumentParser(
        description="Run CTDP interactive visualization app",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path /data/atlas/ctdp_20250121_143022
  uv run streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path /data/atlas/ctdp_20250121_143022
        """,
    )
    parser.add_argument(
        "--path", "-p", type=str, required=True, help="Path to CTDP cache directory"
    )
    args = parser.parse_args()

    cache_dir = Path(args.path)

    if not cache_dir.exists():
        print(f"❌ Error: Cache directory not found: {cache_dir}")
        print(f"   Create a CTDP state first using create_ctdp()")
        exit(1)

    if not (cache_dir / "metadata.json").exists():
        print(f"❌ Error: Not a valid CTDP cache directory: {cache_dir}")
        print(f"   Missing metadata.json file")
        exit(1)

    print(f"✓ Loading CTDP from: {cache_dir}")
    run_app(cache_dir)


if __name__ == "__main__":
    main()
