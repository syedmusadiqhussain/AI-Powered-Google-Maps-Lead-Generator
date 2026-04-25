import os
import asyncio
import re
from typing import Any, Awaitable, Callable, Dict, List, Optional, TypedDict
from urllib.parse import urlparse
import dns.resolver
from .data_export import update_business_data, load_excel_data
from .web_scraper import (
    WebsiteScraper,
    scrape_website,
    extract_emails_from_content,
    find_relevant_links,
)
from .utils import ainvoke_llm
from .places_api import serper_web_search


# Type definitions for structured data
class BusinessInfo(TypedDict):
    """TypedDict defining the structure of business information response"""
    facebook: str # Facebook link
    twitter: str # Twitter link
    instagram: str # Instagram link
    contact: str # Contact page link

class EmailsResponse(TypedDict):
    """TypedDict for email response"""
    emails: List[str]

_EMAIL_SPLIT_PATTERN = re.compile(r"\s*\|\|\s*|\s*,\s*|\s*;\s*")
_EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

_FREE_EMAIL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "aol.com",
    "icloud.com",
    "proton.me",
    "protonmail.com",
    "gmx.com",
    "yandex.com",
    "yandex.ru",
    "mail.com",
}


def _normalize_emails(value: Any) -> list[str]:
    """
    Normalize an email field into a clean list of lowercase emails.
    """
    if not value:
        return []
    if isinstance(value, list):
        raw = " || ".join(str(v) for v in value if v)
    else:
        raw = str(value)
    parts = [p.strip() for p in _EMAIL_SPLIT_PATTERN.split(raw) if p and p.strip()]
    emails: list[str] = []
    for part in parts:
        for e in _EMAIL_PATTERN.findall(part):
            emails.append(e.lower())
    deduped = list(dict.fromkeys(emails))
    return deduped


def _email_domain(email: str) -> str:
    """
    Extract domain from an email address.
    """
    if not email or "@" not in email:
        return ""
    return email.split("@", 1)[1].strip().lower()


def _has_mx_record(domain: str) -> bool:
    """
    Return True if the domain has at least one MX record.
    """
    if not domain:
        return False
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=3.0)
        return bool(list(answers))
    except Exception:
        return False


def classify_email_health(email_value: Any) -> str:
    """
    Classify email health as: valid | risky | unknown.

    - valid: domain has MX and is not a free email domain
    - risky: email is from a common free provider (gmail/yahoo/etc.)
    - unknown: cannot determine (no email or MX check fails)
    """
    emails = _normalize_emails(email_value)
    if not emails:
        return "unknown"

    any_risky = False
    for email in emails:
        domain = _email_domain(email)
        if not domain:
            continue
        if domain in _FREE_EMAIL_DOMAINS:
            any_risky = True
            continue
        if _has_mx_record(domain):
            return "valid"
    return "risky" if any_risky else "unknown"


def extract_first_linkedin_url(serper_response: Dict[str, Any]) -> str:
    """
    Extract the first LinkedIn profile/company URL from Serper web search results.
    """
    organic = serper_response.get("organic") or []
    for item in organic:
        url = (item.get("link") or item.get("url") or "").strip()
        if not url:
            continue
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        if "linkedin.com" in host and (path.startswith("/company/") or path.startswith("/in/")):
            return url
    return ""


async def add_linkedin_profiles(
    excel_file: str,
    serper_api_key: str,
    concurrency: int = 3,
    progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    log_callback: Optional[Callable[[str], Awaitable[None]]] = None,
) -> str:
    """
    Populate the 'linkedin_url' column by searching Serper for each business's LinkedIn.
    """
    df, file_path = load_excel_data(excel_file)
    if "linkedin_url" not in df.columns:
        df["linkedin_url"] = ""

    def extract_city(address: str) -> str:
        """
        Best-effort city extraction from an address string.
        """
        parts = [p.strip() for p in str(address).split(",") if p and str(p).strip()]
        if len(parts) >= 2:
            return parts[-2]
        return parts[0] if parts else ""

    rows: list[tuple[int, str, str]] = []
    for index, row in df.iterrows():
        name = str(row.get("name", "")).strip()
        city = extract_city(str(row.get("address", "")).strip())
        current = str(row.get("linkedin_url", "")).strip()
        if name and not current:
            rows.append((index, name, city))

    total = len(rows)
    if total == 0:
        return file_path

    semaphore = asyncio.Semaphore(max(1, int(concurrency)))
    lock = asyncio.Lock()
    completed = 0

    async def run_one(index: int, name: str, city: str):
        nonlocal completed
        linkedin_url: str = ""
        try:
            async with semaphore:
                query = f"\"{name}\" \"{city}\" site:linkedin.com/company OR site:linkedin.com/in"
                result = await asyncio.to_thread(serper_web_search, query, serper_api_key, 5)
                organic = result.get("organic") or []
                for item in organic:
                    link = str(item.get("link", "")).strip()
                    if "linkedin.com/company" in link or "linkedin.com/in" in link:
                        linkedin_url = link
                        break
        except Exception as e:
            if log_callback:
                await log_callback(f"LinkedIn search error: {name} — {e}")
        async with lock:
            completed += 1
            df.at[index, "linkedin_url"] = linkedin_url or ""
            try:
                df.to_excel(file_path, index=False)
            except Exception as e:
                if log_callback:
                    await log_callback(f"Error saving Excel file during LinkedIn search: {e}")
            if progress_callback:
                await progress_callback(total, completed - 1, name)
            if log_callback:
                status = "found LinkedIn" if linkedin_url else "no LinkedIn found"
                await log_callback(f"[{completed}/{total}] LinkedIn: {name} — {status}")

    await asyncio.gather(*(run_one(i, n, l) for i, n, l in rows))
    try:
        df.to_excel(file_path, index=False)
    except Exception as e:
        if log_callback:
            await log_callback(f"Error saving Excel file: {e}")
    return file_path

async def analyze_business_links(
    links_dict: Dict[str, List[str]], 
    business_name: str, 
    business_location: str, 
    business_url: str,
    llm_model: str | None = None,
    openrouter_api_key: str | None = None,
):
        # Create system prompt for the AI
    system_prompt = f"""
You are an expert at identifying the correct business information from scraped web data.
Your task is to analyze potential social media links for a business,
and determine which ones are most likely the official ones.

## Business Information
- Business Name: {business_name}
- Business Location: {business_location}
- Business Website URL: {business_url}

Provide only the most probable link for each category.
If no valid option exists for a category, return an empty string.
"""

    # Create user message with all the context
    user_message = f"""
    Potential Facebook links: {links_dict.get('facebook', [])}
    Potential Twitter links: {links_dict.get('twitter', [])}
    Potential Instagram links: {links_dict.get('instagram', [])}
    Potential Contact page links: {links_dict.get('contact', [])}
    """
    
    # Invoke LLM to get structured response
    response = await ainvoke_llm(
        model=llm_model or os.getenv("LLM_MODEL", "gpt-4.1-mini"),
        system_prompt=system_prompt,
        user_message=user_message,
        openrouter_api_key=openrouter_api_key,
        response_format=BusinessInfo,
        temperature=0.1
    )
    
    return response

async def analyze_business_emails(
    emails: List[str], 
    business_name: str, 
    business_location: str, 
    business_url: str,
    llm_model: str | None = None,
    openrouter_api_key: str | None = None,
):
    system_prompt = f"""
Identify all relevant business contact emails. Prioritize general contact addresses (such as info@ or contact@) and emails of key personnel that use the business's domain. Exclude department-specific ones (e.g., press, events) unless no main contact is available.

If no domain-based business emails are found, provide any available emails, including personal or free-domain addresses (e.g., Gmail, Yahoo) as fallback contacts.

## Business Information
- Business Name: {business_name}
- Business Location: {business_location}
- Business Website URL: {business_url}

**If only a single valid email is found, just return it.**
"""

    # Create user message with all the context
    user_message = f"Potential emails: {list(emails)}"
    
    # Invoke LLM to get structured response
    response = await ainvoke_llm(
        model=llm_model or os.getenv("LLM_MODEL", "gpt-4.1-mini"),
        system_prompt=system_prompt,
        user_message=user_message,
        openrouter_api_key=openrouter_api_key,
        response_format=EmailsResponse,
        temperature=0.1
    )
    
    return response

async def get_business_info(
    business_url: str,
    business_name: str,
    business_location: str,
    scraper: WebsiteScraper | None = None,
    llm_model: str | None = None,
    openrouter_api_key: str | None = None,
):
    """
    Get comprehensive business information by scraping the website and analyzing the data.
    
    Args:
        business_url (str): URL of the business website
        business_name (str): Name of the business
        business_location (str): Location of the business
        
    Returns:
        Dict[str, str]: Business info with social media links and email
    """
    # Scrape the main website
    content, links = await scrape_website(business_url, extract_links=True, scraper=scraper)
    if not content:
        return {}
    social_links = find_relevant_links(links)
    emails = extract_emails_from_content(content)
    
    # Analyze the identified links
    links_result = await analyze_business_links(
        social_links,
        business_name,
        business_location,
        business_url,
        llm_model=llm_model,
        openrouter_api_key=openrouter_api_key,
    )
    
    if emails:
        emails_result = await analyze_business_emails(
            emails,
            business_name,
            business_location,
            business_url,
            llm_model=llm_model,
            openrouter_api_key=openrouter_api_key,
        )
    else:
        emails_result = {'emails': ''}

    # If no email found and we have a contact link, try scraping the contact page
    if (not emails_result.get('emails')) and social_links.get('contact'):
        contact_url = social_links['contact'][0]  # Take the first contact link
        if contact_url != business_url:
            contact_content, _ = await scrape_website(contact_url, extract_links=False, scraper=scraper)
            contact_emails = extract_emails_from_content(contact_content)
            if contact_emails:
                # Re-analyze with new emails
                emails_result = await analyze_business_emails(
                    contact_emails,
                    business_name,
                    business_location,
                    business_url,
                    llm_model=llm_model,
                    openrouter_api_key=openrouter_api_key,
                )

    # Return combined information
    email_joined = " || ".join(emails_result.get('emails', ''))
    return {
        'facebook': links_result.get('facebook', ''),
        'twitter': links_result.get('twitter', ''),
        'instagram': links_result.get('instagram', ''),
        'contact': links_result.get('contact', ''),
        'email': email_joined,
        'email_health': classify_email_health(email_joined),
    }

async def process_businesses(
    excel_file: str,
    progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    log_callback: Optional[Callable[[str], Awaitable[None]]] = None,
    llm_model: str | None = None,
    openrouter_api_key: str | None = None,
    concurrency: int = 3,
    headless: bool = True,
    only_missing_email: bool = False,
):
    """
    Process a list of businesses to extract detailed information and update the Excel file.
    
    Args:
        excel_file (str): Path to the Excel file to update
        
    Returns:
        List[Dict]: Enhanced business data with extracted information
    """
    # Load the Excel file into a DataFrame
    df, file_path = load_excel_data(excel_file)

    rows_to_process = []
    for index, row in df.iterrows():
        name = row.get("name", "")
        url = row.get("website", "")
        location = row.get("address", "")
        email_value = str(row.get("email", "")).strip()
        already_searched = str(row.get("searched", "")).strip()
        if not url:
            continue
        if only_missing_email:
            if not email_value:
                rows_to_process.append((index, name, url, location))
        else:
            if already_searched != "YES":
                rows_to_process.append((index, name, url, location))

    total = len(rows_to_process)
    if total == 0:
        return file_path

    semaphore = asyncio.Semaphore(max(1, int(concurrency)))
    update_lock = asyncio.Lock()
    completed = 0

    async with WebsiteScraper(headless=headless) as scraper:
        async def run_one(index: int, name: str, url: str, location: str):
            nonlocal completed
            info = {}
            try:
                async with semaphore:
                    info = await get_business_info(
                        url,
                        name,
                        location,
                        scraper=scraper,
                        llm_model=llm_model,
                        openrouter_api_key=openrouter_api_key,
                    )
            except Exception as e:
                if log_callback and callable(log_callback):
                    await log_callback(f"Error processing {name}: {e}")
            async with update_lock:
                completed += 1
                if progress_callback and callable(progress_callback):
                    await progress_callback(total, completed - 1, name)
                if log_callback and callable(log_callback):
                    email_value = str(info.get("email", "")).strip()
                    status = "found email" if email_value else "no email found"
                    await log_callback(f"[{completed}/{total}] Scraping: {name} — {status}")
                update_business_data(df, index, info)

        await asyncio.gather(*(run_one(i, n, u, l) for i, n, u, l in rows_to_process))
    
    # Save the updated DataFrame back to Excel
    try:
        df.to_excel(file_path, index=False)
    except Exception as e:
        print(f"Error saving Excel file: {e}")
    return file_path
