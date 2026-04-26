# data-class: public-aggregate
import json
import os
from datetime import datetime, timezone

_DOCS_DIR   = os.path.join(os.path.dirname(__file__), '..', 'docs')
_STATE_FILE = os.path.join(_DOCS_DIR, '.pipeline_status.json')
_MD_FILE    = os.path.join(_DOCS_DIR, 'STATUS.md')

_MD_HEADER = (
    "# Pipeline Status\n\n"
    "| Service | Last Check-in (UTC) | Status |\n"
    "|---|---|---|\n"
)
_MD_FOOTER = "\n_Updated automatically by the pipeline._\n"


def update_status(service: str, status: str) -> None:
    """Record PASS or FAIL for `service` and rewrite docs/STATUS.md."""
    try:
        state = {}
        if os.path.exists(_STATE_FILE):
            with open(_STATE_FILE) as f:
                state = json.load(f)

        state[service] = {
            "last_checkin": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": status,
        }

        os.makedirs(_DOCS_DIR, exist_ok=True)
        with open(_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)

        rows = "".join(
            f"| {svc} | {v['last_checkin']} | {v['status']} |\n"
            for svc, v in sorted(state.items())
        )
        with open(_MD_FILE, 'w') as f:
            f.write(_MD_HEADER + rows + _MD_FOOTER)

    except Exception as e:
        print(f"[status] Warning: could not update STATUS.md: {e}")
