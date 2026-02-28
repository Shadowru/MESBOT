# tests/conftest.py
import sys
import os
from pathlib import Path
from unittest.mock import MagicMock, patch


# Добавляем корень проекта в sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Устанавливаем ДО импорта bot.py
os.environ["TELEGRAM_TOKEN"] = "1234567890:ABCdefGHIjklMNOpqrsTUVwxyz123456789"
os.environ["OPENAI_API_KEY"] = "fake-key"
os.environ["GOOGLE_SHEET_URL"] = "https://docs.google.com/spreadsheets/d/fake"
os.environ["GOOGLE_CREDS_PATH"] = "fake_creds.json"

# Мокаем Google credentials и gspread ДО импорта bot.py
mock_worksheet = MagicMock()
mock_worksheet.get_all_records.return_value = []
mock_worksheet.col_values.return_value = []
mock_worksheet.append_row.return_value = None
mock_worksheet.delete_rows.return_value = None

mock_sheet = MagicMock()
mock_sheet.worksheet.return_value = mock_worksheet

mock_gs_client = MagicMock()
mock_gs_client.open_by_url.return_value = mock_sheet

patch("gspread.authorize", return_value=mock_gs_client).start()
patch(
    "oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_name",
    return_value=MagicMock(),
).start()