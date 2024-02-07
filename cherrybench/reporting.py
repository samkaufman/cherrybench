import gspread
import logging
import platform
import pathlib
import pydrive2.auth
import pydrive2.drive
import oauth2client.service_account
import mimetypes
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
        uploaded_url = self._upload_dir(
            local_dir,
        )
        row = [
            str(start_time),
            self.hostname,
            job.name,
            job.size,
            job.batch_size,
            job.backend_name,
            runtime_secs,
            "",
            ", ".join(f"{s:.8f}" for s in runtime_samples),
            uploaded_url,
            "",
            "",
            "",
            str(is_rt),
        ]
        self.sheet.append_row(row, value_input_option="USER_ENTERED")
        logger.debug("Logged row to Google Sheets: %s", row)

    def _upload_dir(
        self,
        local_dir: pathlib.Path,
        parent_id: Optional[str] = None,
    ):
        assert local_dir.is_dir()

        # Get root folder in Drive based on provided name
        if not parent_id:
            remote_root_candidates = self.drive.ListFile(
                {"q": f"title = '{self.remote_root_name}' and trashed = False"}
            ).GetList()
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
        root_item.Upload()

        for entry in local_dir.iterdir():
            if entry.is_file():
                # Upload file
                file_meta = {"title": entry.name, "parents": [{"id": root_item["id"]}]}
                guess = mimetypes.guess_type(entry)
                if guess[0]:
                    file_meta["mimeType"] = guess[0]
                f = self.drive.CreateFile(file_meta)
                f.SetContentFile(str(entry.absolute()))
                f.Upload()
            else:
                assert entry.is_dir()
                self._upload_dir(entry, parent_id=root_item["id"])

        return root_item["alternateLink"]
