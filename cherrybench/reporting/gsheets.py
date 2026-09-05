import logging
import mimetypes
import pathlib
import platform
import random
import time
import types
from typing import Any, Optional

from ..results import BenchmarkResult, SuccessResult, UnsatisfiableResult
from .stats import median_gflops_per_sec

logger = logging.getLogger(__name__)


def _import_google_reporting_dependencies():
    try:
        import google.oauth2.service_account as google_service_account
        import gspread
        import oauth2client.service_account as oauth2_service_account
        import pydrive2.auth as pydrive2_auth
        import pydrive2.drive as pydrive2_drive
        import requests
    except ImportError as e:
        raise RuntimeError(
            "Google Sheets reporting dependencies are not installed"
        ) from e

    return types.SimpleNamespace(
        google_service_account=google_service_account,
        gspread=gspread,
        oauth2_service_account=oauth2_service_account,
        pydrive2_auth=pydrive2_auth,
        pydrive2_drive=pydrive2_drive,
        requests=requests,
    )


def _is_retryable_google_reporting_error(error):
    deps = _import_google_reporting_dependencies()
    return isinstance(
        error,
        (
            deps.gspread.exceptions.APIError,
            deps.requests.exceptions.ConnectionError,
            deps.requests.exceptions.Timeout,
        ),
    )


class GSheetsReporter:
    def __init__(self, google_key_file: pathlib.Path, gsheet_name, remote_root_name):
        deps = _import_google_reporting_dependencies()

        self.hostname = platform.node()

        creds = deps.google_service_account.Credentials.from_service_account_file(
            google_key_file,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        self.gc = deps.gspread.Client(creds)
        self.sheet = self.gc.open(gsheet_name).worksheet("Log")
        self.remote_root_name = remote_root_name
        self._existing_entries_cache: Optional[set[tuple]] = None

        gauth = deps.pydrive2_auth.GoogleAuth()
        gauth.auth_method = "service"
        gauth.credentials = (
            deps.oauth2_service_account.ServiceAccountCredentials.from_json_keyfile_name(
                google_key_file, "https://www.googleapis.com/auth/drive"
            )
        )
        self.drive = deps.pydrive2_drive.GoogleDrive(gauth)

    def log_result(
        self,
        start_time,
        job,
        result: BenchmarkResult,
        is_rt: bool,
        local_dir: pathlib.Path,
    ):
        match result:
            case SuccessResult(
                fastest_runtime_secs=runtime_secs, runtime_samples=runtime_samples
            ):
                status, reason = "success", ""
            case UnsatisfiableResult(reason=reason):
                runtime_secs, runtime_samples = None, ()
                status, reason = "unsatisfiable", reason or ""
            case _:
                raise TypeError(f"Unknown benchmark result: {type(result).__name__}")
        uploaded_url = self._upload_dir(local_dir)
        median_gflops = median_gflops_per_sec(job.gflops, runtime_samples)
        row = [
            str(start_time),
            self.hostname,
            job.name,
            job.size,
            job.batch_size,
            job.backend_name,
            "" if runtime_secs is None else runtime_secs,
            "" if median_gflops is None else median_gflops,
            ", ".join(f"{s:.8f}" for s in runtime_samples),
            uploaded_url,
            "",
            "",
            "",
            str(is_rt),
            status,
            reason or "",
        ]

        # Retry the sheet append operation with exponential backoff
        _retry_with_backoff(
            lambda: self.sheet.append_row(row, value_input_option="USER_ENTERED")
        )
        logger.debug("Logged row to Google Sheets: %s", row)

        self._invalidate_cache()

    def has_existing_entry(self, job) -> bool:
        if self._existing_entries_cache is None:
            self._load_existing_entries_cache()
        job_key = (
            self.hostname,
            job.name,
            str(job.size),
            str(job.batch_size),
            job.backend_name,
        )
        return job_key in (self._existing_entries_cache or set())

    def _load_existing_entries_cache(self):
        """Load all existing entries from the sheet into memory cache."""
        logger.debug("Loading existing entries cache from Google Sheets")

        # Get all records from the sheet (skip header row)
        all_records = _retry_with_backoff(lambda: self.sheet.get_all_values())
        if not all_records:
            self._existing_entries_cache = set()
            return

        # Skip the header row
        data_rows = all_records[1:] if len(all_records) > 1 else []

        # Create cache of existing entries
        # Row format: [start_time, hostname, job_name, job_size, batch_size, backend_name, ...]
        self._existing_entries_cache = set()
        for row in data_rows:
            if len(row) >= 6:  # Ensure we have enough columns
                try:
                    job_key = tuple(
                        row[1:6]
                    )  # hostname, job_name, job_size, batch_size, backend_name
                    self._existing_entries_cache.add(job_key)
                except (ValueError, IndexError) as e:
                    logger.error(
                        "Skipping malformed row in sheet: %s (error: %s)", row, e
                    )
                    continue

        logger.debug(
            "Loaded %d existing entries into cache", len(self._existing_entries_cache)
        )

    def _invalidate_cache(self):
        """Invalidate the existing entries cache to force reload on next check."""
        self._existing_entries_cache = None
        logger.debug("Invalidated existing entries cache")

    def _upload_dir(
        self,
        local_dir: pathlib.Path,
        parent_id: Optional[str] = None,
    ):
        assert local_dir.is_dir()

        # Get root folder in Drive based on provided name
        if not parent_id:
            remote_root_candidates = _retry_with_backoff(
                lambda: self.drive.ListFile(
                    {"q": f"title = '{self.remote_root_name}' and trashed = False"}
                ).GetList(),
            )
            if not remote_root_candidates:
                raise ValueError(
                    f"Found no folders with title '{self.remote_root_name}'"
                )
            if len(remote_root_candidates) > 1:
                raise ValueError(
                    f"Found multiple folders with title '{self.remote_root_name}'"
                )
            parent_id = remote_root_candidates[0]["id"]

        # Create new remote subdirectory corresponding to the top of local_dir
        root_meta: dict[str, Any] = {
            "title": local_dir.name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        root_meta["parents"] = [{"id": parent_id}]
        root_item = self.drive.CreateFile(root_meta)
        _retry_with_backoff(lambda: root_item.Upload())

        for entry in local_dir.iterdir():
            if entry.is_file():
                # Upload file
                file_meta = {"title": entry.name, "parents": [{"id": root_item["id"]}]}
                guess = mimetypes.guess_type(entry)
                if guess[0]:
                    file_meta["mimeType"] = guess[0]
                f = self.drive.CreateFile(file_meta)
                f.SetContentFile(str(entry.absolute()))
                _retry_with_backoff(lambda: f.Upload())
            else:
                assert entry.is_dir()
                self._upload_dir(entry, parent_id=root_item["id"])

        return root_item["alternateLink"]


def _retry_with_backoff(func, max_retries=5, base_delay=3.0, max_delay=60.0):
    """Retry a function with exponential backoff after transient reporting errors.

    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            if not _is_retryable_google_reporting_error(e):
                raise
            if attempt == max_retries:
                logger.error(f"All retry attempts failed. Last reporting error: {e}")
                raise

            # Calculate delay with exponential backoff and jitter
            delay = min(base_delay * (2**attempt), max_delay)
            jitter = random.uniform(0.1, 0.3) * delay
            total_delay = delay + jitter

            logger.warning(
                f"API call failed (attempt {attempt + 1}/{max_retries + 1}): {e}"
            )
            logger.info(f"Retrying in {total_delay:.2f} seconds...")
            time.sleep(total_delay)
