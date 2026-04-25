import os
import re
from langchain_openai import ChatOpenAI
from datetime import datetime

# User agents for web scraping
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
]

_INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]')

def get_current_date():
    return datetime.now().strftime("%Y-%m-%d_%H-%M")

def sanitize_filename_component(value: str, replacement: str = "_") -> str:
    if value is None:
        return "output"
    value = str(value).strip()
    if not value:
        return "output"
    value = re.sub(r"\s+", replacement, value)
    value = _INVALID_FILENAME_CHARS.sub(replacement, value)
    value = value.strip(replacement).strip()
    return value or "output"

async def ainvoke_llm(
    model,  # Specify the model name from OpenRouter
    system_prompt,
    user_message,
    openrouter_api_key=None,
    response_format=None,
    temperature=0.1
):
    llm = ChatOpenAI(
        model=model, 
        temperature=temperature,
        openai_api_key=openrouter_api_key or os.getenv("OPENROUTER_API_KEY"),
        openai_api_base="https://openrouter.ai/api/v1",
    )
    
    # If Response format is provided, use structured output
    if response_format:
        llm = llm.with_structured_output(response_format)
    
    # Prepare messages
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message}
    ]
    
    # Invoke LLM asynchronously
    response = await llm.ainvoke(messages)
    
    return response if response_format else response.content  # Return structured response or string
