import gspread
import logging
import platform
import pathlib
import pydrive2.auth
import pydrive2.drive
import oauth2client.service_account
import mimetypes
import time
import random
import google.oauth2.service_account
from typing import Any, Optional

logger = logging.getLogger(__name__)


class GSheetsReporter:
    def __init__(self, google_key_file: pathlib.Path, gsheet_name, remote_root_name):
        self.hostname = platform.node()

        creds = google.oauth2.service_account.Credentials.from_service_account_file(
            google_key_file,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        self.gc = gspread.Client(creds)
        self.sheet = self.gc.open(gsheet_name).worksheet("Log")
        self.remote_root_name = remote_root_name

        gauth = pydrive2.auth.GoogleAuth()
        gauth.auth_method = "service"
        gauth.credentials = oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_name(
            google_key_file, "https://www.googleapis.com/auth/drive"
        )
        self.drive = pydrive2.drive.GoogleDrive(gauth)

    def log_result(
        self,
        start_time,
        job,
        runtime_secs: float,
        runtime_samples,
        is_rt: bool,
        local_dir: pathlib.Path,
    ):
        uploaded_url = self._upload_dir(local_dir)

        median_gflops_per_sec = ""
        if job.gflops is not None and runtime_samples:
            gflops_per_sec_samples = [job.gflops / s for s in runtime_samples if s > 0]
            n = len(gflops_per_sec_samples)
            if n:
                sorted_samples = sorted(gflops_per_sec_samples)
                mid = n // 2
                if n % 2 == 1:
                    median_gflops_per_sec = sorted_samples[mid]
                else:
                    median_gflops_per_sec = (
                        sorted_samples[mid - 1] + sorted_samples[mid]
                    ) / 2

        row = [
            str(start_time),
            self.hostname,
            job.name,
            job.size,
            job.batch_size,
            job.backend_name,
            runtime_secs,
            median_gflops_per_sec,
            ", ".join(f"{s:.8f}" for s in runtime_samples),
            uploaded_url,
            "",
            "",
            "",
            str(is_rt),
        ]

        # Retry the sheet append operation with exponential backoff
        _retry_with_backoff(
            lambda: self.sheet.append_row(row, value_input_option="USER_ENTERED")
        )
        logger.debug("Logged row to Google Sheets: %s", row)

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
    """Retry a function with exponential backoff upon `gspread` API errors.

    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            if attempt == max_retries:
                logger.error(f"All retry attempts failed. Last API error: {e}")
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
