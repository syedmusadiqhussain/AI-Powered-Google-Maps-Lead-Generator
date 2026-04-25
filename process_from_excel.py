import asyncio
import sys
from src.business_info import process_businesses
from dotenv import load_dotenv


load_dotenv()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python process_from_excel.py <excel_file_path_or_name>")
    excel_file = sys.argv[1]
    asyncio.run(process_businesses(excel_file))
