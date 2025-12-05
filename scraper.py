from typing import Dict

from bs4 import BeautifulSoup


def scrape_html(url: str, html: str, status_code: int) -> Dict:
    """
    Extract core information from an HTML page for JSON storage.

    Returns a dict shaped like:
        {
            'url': url,
            'title': title,
            'status_code': status_code,
            'content': <first 500 chars of text>,
        }
    """
    soup = BeautifulSoup(html, "html.parser")

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    text_content = soup.get_text(separator=" ", strip=True)

    data = {
        "url": url,
        "title": title,
        "status_code": status_code,
        "content": text_content[:500],
    }

    return data


