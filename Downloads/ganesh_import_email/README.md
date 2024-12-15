Ganesh Feature Branch

This branch (`ganesh-feature`) focuses on automating and streamlining inventory management for Ganesh Mills. It includes functionalities for processing inventory data, integrating Gmail API for automated email attachment handling, and interacting with the BigCommerce API for real-time inventory adjustments.

---

Purpose

The `ganesh-feature` branch is designed to:
1. Download inventory files sent via email.
2. Process and clean the data using Python.
3. Upload the processed data to a PostgreSQL database.
4. Synchronize inventory levels with BigCommerce through API calls.

This branch is isolated to allow for testing and staging changes before merging into the `main` branch.

---

Files in This Branch

1. `deskgmail_latest.py`
   - Authenticates with Gmail via OAuth 2.0.
   - Downloads the latest inventory report (Excel file) from specific emails.
   - Processes the Excel data and uploads it to the PostgreSQL database.

2. `bigapi.py`
   - Fetches products and variants from BigCommerce.
   - Updates inventory levels based on the database records.
   - Logs inventory adjustments for audit purposes.

3. `deskgmail_all.py` (Optional)
   - A broader version of `deskgmail_latest.py` for handling multiple Gmail queries and attachments simultaneously.

---

Setup Instructions

Prerequisites:
- Python: Version 3.8 or above.
- PostgreSQL: A running database instance.
- BigCommerce API: Access token and API endpoint.
- Gmail API: Credentials for authentication.

Environment Variables:
Create a `.env` file in the root directory with the following:

Gmail API:
GMAIL_SCOPES="https://www.googleapis.com/auth/gmail.readonly"
GMAIL_CREDENTIALS_FILE="path/to/credentials.json"
GMAIL_TOKEN_FILE="path/to/token.json"

PostgreSQL:
DB_NAME="your_database_name"
DB_USER="your_database_user"
DB_PASSWORD="your_database_password"
DB_HOST="localhost"
DB_PORT="5432"

BigCommerce API:
BIGCOMMERCE_API_PATH="https://api.bigcommerce.com/stores/your-store-id/v3"
BIGCOMMERCE_ACCESS_TOKEN="your-access-token"

---

Usage Instructions

Running `deskgmail_latest.py`:
1. Authenticate with Gmail to fetch inventory reports.
   python deskgmail_latest.py
2. The script will:
   - Download the latest email attachment.
   - Process the Excel file.
   - Upload the cleaned data to PostgreSQL.

Running `bigapi.py`:
1. Ensure the database is updated with inventory data.
2. Synchronize BigCommerce inventory levels:
   python bigapi.py

---

Testing and Merging

- Testing Area: This branch serves as a staging area for new features and bug fixes.
- Merge Policy: Changes will be reviewed and tested in `ganesh-feature` before merging into the `main` branch.

---

Notes

- Keep all sensitive credentials and tokens in the `.env` file.
- Use this branch for all Ganesh Mills-related features until they are stable and ready for deployment.

---

Contact

For questions or support, reach out to [Judson Uhre] at [judduhre@gmail.com].
