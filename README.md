# ChartHop Webhook

Integration service for ChartHop with Teamtailor, Culture Amp, Runn, and other HR systems.

## Quick Test

### Testing Job Compensation Fields

To test the `ch_get_job_compensation_fields` function and CTC calculation:

```bash
# Set required environment variables
export CH_API="https://api.charthop.com"
export CH_ORG_ID="creai"
export CH_API_TOKEN="your_charthop_api_token"
export CH_JOB_SCHEME_FIELD_API="esquemaDeContratacin"  # optional, defaults to esquemaDeContratacin

# Set PYTHONPATH to include the repo
export PYTHONPATH="$PWD"

# Test importing the function
python3 -c "from app.clients.charthop import ch_get_job_compensation_fields; print('Import OK')"

# Test fetching job compensation fields directly
python3 - <<'PY'
from app.clients.charthop import ch_get_job_compensation_fields
data = ch_get_job_compensation_fields("670405b2c8e3d13247cfef5d")
print(data)
PY

# Or use the verification script with CTC calculation
python3 tools/check_job_comp.py 670405b2c8e3d13247cfef5d

# Expected output:
# {
#   "base": 108000.0,
#   "scheme": "Ontop",
#   "currency": "USD",
#   "employment": "CONTRACT",
#   "ctc": 108720.0
# }
```

### CTC Calculation Formula

The `tools/check_job_comp.py` script calculates CTC (Cost to Company) using:

```
CTC = base + fee
```

Where fee is:
- **720** if esquema == "Ontop"
- **240** if esquema == "voiz" (case-insensitive)
- **0** otherwise

For example, with base=108000.0 and esquema="Ontop":
```
CTC = 108000 + 720
CTC = 108720.0
```

## Development

### Prerequisites

- Python 3.11+
- ChartHop API credentials
- Required environment variables (see `.env.example` if available)

### Installation

```bash
pip install -r requirements.txt  # if available
```

### Environment Variables

- `CH_API`: ChartHop API base URL (default: https://api.charthop.com)
- `CH_ORG_ID`: Your ChartHop organization ID
- `CH_API_TOKEN`: ChartHop API authentication token
- `CH_JOB_SCHEME_FIELD_API`: Custom field name for hiring scheme (default: esquemaDeContratacin)
- `CH_JOB_CTC_CODES`: Custom field codes for CTC lookup (comma-separated)

## Bug Fix: ch_get_job_compensation_fields Import Issue

### Root Cause

The `ch_get_job_compensation_fields` function existed in `app/clients/charthop.py` but was not being exported properly in `app/clients/__init__.py`. Additionally, the function needed normalization improvements:

1. **Base extraction**: The function could return a money object dict instead of extracting the numeric `amount` value
2. **Esquema normalization**: When the hiring scheme field came as a list, the first element wasn't being extracted

### Fix Applied

1. **Updated `app/clients/__init__.py`**: Added explicit imports and `__all__` declaration to properly export the function
2. **Enhanced `ch_get_job_compensation_fields`**:
   - Added logic to extract `amount` from money objects (`{"currency":"USD","amount":108000.0}`)
   - Added normalization to handle list values for esquema field (takes first element)
   - Added type conversion and validation for both base and esquema fields

3. **Created verification script**: `tools/check_job_comp.py` for easy testing and CTC calculation

These changes ensure the function can be imported and returns properly normalized data for downstream consumption.
