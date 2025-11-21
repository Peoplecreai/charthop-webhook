#!/usr/bin/env python3
"""
Quick test script to verify ChartHop job compensation fields retrieval and CTC calculation.

Usage:
    export CH_API="https://api.charthop.com"
    export CH_ORG_ID="creai"
    export CH_API_TOKEN="your_token"
    export CH_JOB_SCHEME_FIELD_API="esquemaDeContratacin"

    python3 tools/check_job_comp.py JOB_ID
    # or
    JOB_ID=670405b2c8e3d13247cfef5d python3 tools/check_job_comp.py
"""

import json
import os
import sys


def calculate_ctc(base: float, esquema: str) -> float:
    """
    Calculate CTC (Cost to Company) based on base compensation and hiring scheme.

    Formula: CTC = base + fee

    Where fee is:
    - 720 if esquema == "Ontop"
    - 240 if esquema == "voiz" (case-insensitive)
    - 0 otherwise

    Args:
        base: Base compensation amount
        esquema: Hiring scheme/esquema de contratación

    Returns:
        Rounded CTC value (2 decimal places)
    """
    if not base:
        return 0.0

    # Normalize esquema
    esquema_normalized = (esquema or "").strip().lower()

    # Determine fee based on esquema
    if esquema_normalized == "ontop":
        fee = 720
    elif esquema_normalized == "voiz":
        fee = 240
    else:
        fee = 0

    # Calculate CTC
    ctc = base + fee

    return round(ctc, 2)


def main():
    # Get job ID from command line argument or environment variable
    job_id = None
    if len(sys.argv) > 1:
        job_id = sys.argv[1].strip()
    else:
        job_id = os.getenv("JOB_ID", "").strip()

    if not job_id:
        print("Error: JOB_ID required. Provide as argument or environment variable.", file=sys.stderr)
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    # Verify required environment variables
    required_vars = ["CH_API", "CH_ORG_ID", "CH_API_TOKEN"]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        print(f"Error: Missing required environment variables: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    # Import after env vars are validated (module needs them at import time)
    try:
        from app.clients.charthop import ch_get_job_compensation_fields
    except ImportError as e:
        print(f"Error importing ch_get_job_compensation_fields: {e}", file=sys.stderr)
        print("Make sure PYTHONPATH includes the repo root.", file=sys.stderr)
        sys.exit(1)

    # Fetch job compensation fields
    try:
        data = ch_get_job_compensation_fields(job_id)
    except Exception as e:
        print(f"Error fetching job compensation fields: {e}", file=sys.stderr)
        sys.exit(1)

    if not data:
        print(f"Error: No data returned for job {job_id}", file=sys.stderr)
        sys.exit(1)

    # Extract fields
    base = data.get("base")
    esquema = data.get("esquema_contratacion")
    currency = data.get("currency")
    employment = data.get("employment")

    # Normalize esquema to string
    if esquema is None:
        esquema_str = None
    elif isinstance(esquema, list):
        # If it's a list, take the first element (should already be handled by the function)
        esquema_str = str(esquema[0]) if esquema else None
    else:
        esquema_str = str(esquema)

    # Calculate CTC
    if base is not None:
        try:
            base_float = float(base)
            ctc = calculate_ctc(base_float, esquema_str or "")
        except (ValueError, TypeError):
            print(f"Error: Invalid base value: {base}", file=sys.stderr)
            sys.exit(1)
    else:
        base_float = None
        ctc = None

    # Build result
    result = {
        "base": base_float,
        "scheme": esquema_str,
        "currency": currency,
        "employment": employment,
        "ctc": ctc,
    }

    # Print JSON result
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
