"""Enqueue a scheduled SignalDeck script without contacting the web application.

Executed by the host's OS cron using the same Python interpreter as SignalDeck.
"""
from __future__ import annotations

import argparse
import logging
import sys

from .services.central_script_runner import ScriptJobStore, script_db_path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Enqueue a scheduled SignalDeck script")
    parser.add_argument("--config", required=True, help="Path to the SignalDeck instance config")
    parser.add_argument("--schedule-id", required=True)
    args = parser.parse_args(argv)
    try:
        store = ScriptJobStore(script_db_path(args.config))
        job_id = store.enqueue_schedule(args.schedule_id)
        print(job_id)
        return 0
    except Exception:
        logging.exception("Could not enqueue scheduled script")
        return 1


if __name__ == "__main__":
    sys.exit(main())
