import streamlit as st
import asyncio
import os
import sys
import glob
import math
import json
import pandas as pd
from typing import Any, Awaitable, Callable, Optional
from src.places_api import search_places, get_coordinates
from src.business_info import add_linkedin_profiles, process_businesses
from src.data_export import save_places_to_excel
from src.utils import get_current_date, sanitize_filename_component
from dotenv import load_dotenv

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

load_dotenv()

# Set page config
st.set_page_config(
    page_title="Google Maps Lead Generator",
    page_icon="🔍",
    layout="wide"
)

# App title and description
st.title("AI-Powered Google Maps Lead Generator")
st.markdown(
    """
    This tool helps you generate leads from Google Maps by:
    1. Searching for businesses matching your criteria
    2. Extracting contact information from their websites
    3. Using AI to find emails and social media profiles
    """
)

def _get_data_dir() -> str:
    """
    Return absolute path to the app's /data folder.
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def _list_excel_files() -> list[str]:
    """
    Return absolute paths of .xlsx files under /data, sorted newest-first.
    """
    data_dir = _get_data_dir()
    if not os.path.exists(data_dir):
        return []
    paths = [
        os.path.join(data_dir, f)
        for f in os.listdir(data_dir)
        if f.lower().endswith(".xlsx")
    ]
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths


@st.cache_data(show_spinner=False)
def _load_excel(path: str) -> pd.DataFrame:
    """
    Load an Excel file into a DataFrame and normalize missing values.
    """
    df = pd.read_excel(path)
    return df.fillna("")

# Sidebar for settings
with st.sidebar:
    st.header("Settings")

    # API Keys
    st.subheader("API Keys")
    if "serper_api_key" not in st.session_state:
        st.session_state.serper_api_key = os.environ.get("SERPER_API_KEY", "")
    if "openrouter_api_key" not in st.session_state:
        st.session_state.openrouter_api_key = os.environ.get("OPENROUTER_API_KEY", "")
    
    # LLM Model Settings
    st.subheader("LLM Model")
    if "llm_model" not in st.session_state:
        st.session_state.llm_model = os.environ.get("LLM_MODEL", "openai/gpt-4.1-mini")

    st.text_input("Serper API Key", type="password", key="serper_api_key")
    st.text_input("OpenRouter API Key", type="password", key="openrouter_api_key")

    st.selectbox(
        "Select LLM Model",
        options=[
            "openai/gpt-4.1-mini",
            "openai/gpt-4o-mini",
            "anthropic/claude-3-haiku",
            "anthropic/claude-3.5-sonnet",
            "deepseek/deepseek-chat",
            "mistral/mistral-large-2"
        ],
        key="llm_model",
    )

    st.subheader("Scraping")
    if "headless" not in st.session_state:
        st.session_state.headless = True
    if "concurrency" not in st.session_state:
        st.session_state.concurrency = 3

    st.checkbox("Headless browser", key="headless")
    st.slider("Concurrency", min_value=1, max_value=10, value=st.session_state.concurrency, key="concurrency")

    st.subheader("Serper Credits")
    selected_places = int(st.session_state.get("num_places", 20))
    estimated_pages = max(1, math.ceil(selected_places / 20))
    st.write(f"Estimated credits for place search: {estimated_pages}")

    st.subheader("Data")
    st.warning("This will delete ONLY .xlsx/.csv/.json files inside the output data folder.")

    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")
    OUTPUT_DIR = os.path.abspath(OUTPUT_DIR)

    project_root = os.path.abspath(os.path.dirname(__file__))
    if not OUTPUT_DIR.startswith(project_root):
        st.error(f"Unsafe output directory detected: {OUTPUT_DIR}")
    else:
        allowed_extensions = [".xlsx", ".csv", ".json"]
        files = [
            f
            for f in glob.glob(os.path.join(OUTPUT_DIR, "*"))
            if os.path.splitext(f)[1].lower() in allowed_extensions
        ]

        st.write("Files that will be deleted:")
        if files:
            for f in files:
                st.write(f)
        else:
            st.write("(none)")

        st.checkbox("I understand and want to delete these files", key="confirm_clear_data")
        if st.button("Confirm Delete Data"):
            if not st.session_state.get("confirm_clear_data"):
                st.error("Please check the confirmation checkbox first.")
            else:
                deleted = 0
                for f in files:
                    try:
                        os.remove(f)
                        deleted += 1
                    except Exception:
                        continue
                st.success(f"{deleted} files deleted")
                st.session_state.excel_path = None
                st.session_state.logs = []
                st.cache_data.clear()

# Initialize session state if not already done
if "excel_path" not in st.session_state:
    st.session_state.excel_path = None
if "logs" not in st.session_state:
    st.session_state.logs = []

if not st.session_state.excel_path:
    excel_files = _list_excel_files()
    if excel_files:
        st.session_state.excel_path = excel_files[0]

async def main_with_progress(
    location: str,
    search_query: str,
    num_pages: int,
    serper_api_key: str,
    openrouter_api_key: str,
    llm_model: str,
    log_callback: Optional[Callable[[str], Awaitable[None]]] = None,
):
    """
    Main function with progress reporting for Streamlit
    """
    # Status placeholder for showing current operation
    status = st.empty()
    
    # Step 1: Get coordinates from location
    status.text("🔍 Getting coordinates for location...")
    coords = get_coordinates(location)
    if not coords:
        st.error("❌ Could not get coordinates for the location. Please check the location name and try again.")
        return None
        
    # Step 2: Search for places using Serper Maps API
    status.text("🔍 Searching for businesses using Serper Maps API...")
    places_data = search_places(search_query, coords, num_pages, api_key=serper_api_key)
    if not places_data:
        st.error("❌ No places found. Try a different search query or location.")
        return None
        
    # Step 3: Save places data to Excel
    status.text("💾 Saving initial data to Excel...")
    excel_filename = f"data_{sanitize_filename_component(search_query)}_{sanitize_filename_component(location)}_{get_current_date()}.xlsx"
    
    # Make sure excel file is saved in the data directory
    file_path = save_places_to_excel(places_data, excel_filename)
    
    # Step 4: Process businesses to get detailed information
    status.text("🌐 Processing businesses to extract detailed information...")
    
    # Create a Streamlit progress bar
    progress_bar = st.progress(0)
    progress_text = st.empty()
    log_box = st.empty()
    
    # Define a custom callback to track progress
    async def progress_callback(total, current, business_name):
        # Update the progress bar
        progress_bar.progress((current + 1) / total)
        progress_text.text(f"Processing: {current + 1}/{total} - {business_name}")

    async def ui_log_callback(message: str):
        st.session_state.logs.append(message)
        log_box.text_area("Logs", value="\n".join(st.session_state.logs[-300:]), height=220)
        if log_callback:
            await log_callback(message)
    
    # Process the businesses with our progress callback
    file_path = await process_businesses(
        file_path,
        progress_callback=progress_callback,
        log_callback=ui_log_callback,
        llm_model=llm_model,
        openrouter_api_key=openrouter_api_key,
        concurrency=st.session_state.concurrency,
        headless=st.session_state.headless,
    )
    
    status.text("✅ Lead generation complete!")
    
    return file_path


tab_scraper, tab_dashboard = st.tabs(["Scraper", "Dashboard"])

with tab_scraper:
    with st.form("search_form"):
        col1, col2 = st.columns(2)

        with col1:
            location = st.text_input("Location (city, address, etc.)", value="New York", key="location")
            search_query = st.text_input(
                "Search Query (e.g., 'coffee shops', 'dentists')",
                value="Real Estate Agents",
                key="search_query",
            )

        with col2:
            st.number_input("Number of Places to Scrape", min_value=20, max_value=1000, value=20, step=20, key="num_places")
            num_pages = max(1, math.ceil(int(st.session_state.num_places) / 20))

        submit_button = st.form_submit_button("Start Lead Generation")

    if submit_button:
        if not st.session_state.serper_api_key or not st.session_state.openrouter_api_key:
            st.error("⚠️ Please set your API keys in the sidebar before starting.")
        else:
            st.session_state.logs = []
            with st.spinner("Starting lead generation..."):
                excel_path = asyncio.run(
                    main_with_progress(
                        st.session_state.location,
                        st.session_state.search_query,
                        num_pages,
                        st.session_state.serper_api_key,
                        st.session_state.openrouter_api_key,
                        st.session_state.llm_model,
                    )
                )
                if excel_path:
                    st.session_state.excel_path = excel_path

    if st.session_state.excel_path and os.path.exists(st.session_state.excel_path):
        st.subheader("Results")

        df = _load_excel(st.session_state.excel_path)

        query_text = st.text_input("Search by business name", value="", key="filter_name")
        email_filter = st.selectbox("Email Filter", options=["All", "Has Email", "No Email"], index=0, key="filter_email")
        health_options = ["All"]
        if "email_health" in df.columns:
            health_options += sorted([v for v in df["email_health"].astype(str).unique().tolist() if v])
        email_health_filter = st.selectbox("Email Health", options=health_options, index=0, key="filter_health")

        filtered_df = df.copy()
        if query_text:
            filtered_df = filtered_df[filtered_df["name"].astype(str).str.contains(query_text, case=False, na=False)]
        if email_filter == "Has Email":
            filtered_df = filtered_df[filtered_df["email"].astype(str).str.strip() != ""]
        elif email_filter == "No Email":
            filtered_df = filtered_df[filtered_df["email"].astype(str).str.strip() == ""]
        if email_health_filter != "All" and "email_health" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["email_health"].astype(str) == email_health_filter]

        st.write(f"Showing {len(filtered_df)} of {len(df)} leads")
        st.dataframe(filtered_df, use_container_width=True)

        with open(st.session_state.excel_path, "rb") as excel_file:
            excel_bytes = excel_file.read()

        st.download_button(
            label="📥 Download Excel File",
            data=excel_bytes,
            file_name=os.path.basename(st.session_state.excel_path),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_button",
        )

        st.download_button(
            label="Export CSV",
            data=filtered_df.to_csv(index=False).encode("utf-8"),
            file_name=os.path.splitext(os.path.basename(st.session_state.excel_path))[0] + ".csv",
            mime="text/csv",
            key="download_csv",
        )

        st.download_button(
            label="Export JSON",
            data=json.dumps(filtered_df.to_dict(orient="records"), ensure_ascii=False, indent=2).encode("utf-8"),
            file_name=os.path.splitext(os.path.basename(st.session_state.excel_path))[0] + ".json",
            mime="application/json",
            key="download_json",
        )

        st.divider()
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("Find LinkedIn Profiles"):
                if not st.session_state.serper_api_key:
                    st.error("⚠️ Please set your Serper API key in the sidebar.")
                else:
                    progress_bar = st.progress(0.0)
                    log_box = st.empty()
                    st.session_state.logs = []

                    async def log_callback(message: str):
                        st.session_state.logs.append(message)
                        log_box.text_area("Logs", value="\n".join(st.session_state.logs[-300:]), height=220)

                    async def progress_callback(total: int, current: int, business_name: str):
                        progress_bar.progress((current + 1) / max(1, total))
                        await log_callback(f"[{current + 1}/{total}] LinkedIn: {business_name}")

                    with st.spinner("Finding LinkedIn profiles..."):
                        asyncio.run(
                            add_linkedin_profiles(
                                st.session_state.excel_path,
                                st.session_state.serper_api_key,
                                concurrency=st.session_state.concurrency,
                                progress_callback=progress_callback,
                                log_callback=log_callback,
                            )
                        )
                    st.cache_data.clear()
                    st.rerun()

        with col_b:
            if st.button("Retry Failed"):
                log_box = st.empty()
                st.session_state.logs = []

                async def log_callback(message: str):
                    st.session_state.logs.append(message)
                    log_box.text_area("Logs", value="\n".join(st.session_state.logs[-300:]), height=220)

                async def progress_callback(total: int, current: int, business_name: str):
                    pass

                with st.spinner("Retrying leads with missing emails..."):
                    asyncio.run(
                        process_businesses(
                            st.session_state.excel_path,
                            progress_callback=progress_callback,
                            log_callback=log_callback,
                            llm_model=st.session_state.llm_model,
                            openrouter_api_key=st.session_state.openrouter_api_key,
                            concurrency=st.session_state.concurrency,
                            headless=st.session_state.headless,
                            only_missing_email=True,
                        )
                    )
                st.cache_data.clear()
                st.rerun()

with tab_dashboard:
    st.subheader("Dashboard")
    excel_files = _list_excel_files()
    if not excel_files:
        st.info("No data files found in /data yet. Run the scraper to generate an Excel file.")
    else:
        selected = st.selectbox(
            "Select a run file",
            options=excel_files,
            format_func=lambda p: os.path.basename(p),
            index=0,
        )
        df = _load_excel(selected)
        total_leads = len(df)
        emails_found = int((df.get("email", "").astype(str).str.strip() != "").sum()) if total_leads else 0
        emails_not_found = total_leads - emails_found

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Leads Collected", total_leads)
        with col2:
            if total_leads:
                st.progress(emails_found / total_leads)
                st.write(f"Emails found: {emails_found} | No email: {emails_not_found}")
            else:
                st.progress(0.0)

        if "address" in df.columns and total_leads:
            locations = df["address"].astype(str).str.split(",").str[-2].fillna("").str.strip()
            counts = locations[locations != ""].value_counts().head(10)
            if not counts.empty:
                st.write("Top Cities/Locations Scraped")
                st.bar_chart(counts)

        history_rows = []
        for path in excel_files:
            try:
                run_df = _load_excel(path)
                history_rows.append(
                    {"run_time": pd.to_datetime(os.path.getmtime(path), unit="s"), "leads": len(run_df)}
                )
            except Exception:
                continue
        if history_rows:
            history_df = pd.DataFrame(history_rows).sort_values("run_time")
            history_df = history_df.set_index("run_time")
            st.write("Leads per Run History")
            st.line_chart(history_df["leads"])
        
