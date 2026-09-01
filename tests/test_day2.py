from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from bs4 import BeautifulSoup

from src.models import AsyncCrawler
from src.parser import HTMLParser


BASE_URL = "https://example.test/docs/index.html"

VALID_HTML = """
<!doctype html>
<html>
  <head>
    <title>Test page</title>
    <meta name="description" content="Page description">
    <meta name="keywords" content="python, asyncio">
  </head>
  <body>
    <main>
      <h1>Main heading</h1>
      <h2>Second heading</h2>
      <h3>Third heading</h3>
      <p class="article">Useful article text</p>

      <a href="/about">About</a>
      <a href="contact">Contact</a>
      <a href="https://external.test/page">External</a>

      <img src="/images/one.png" alt="First image">
      <img src="images/two.png">

      <table>
        <tr><th>Name</th><th>Value</th></tr>
        <tr><td>alpha</td><td>1</td></tr>
      </table>

      <ul><li>First</li><li>Second</li></ul>
      <ol><li>One</li><li>Two</li></ol>
    </main>
  </body>
</html>
"""


class HTMLParserTests(unittest.IsolatedAsyncioTestCase):
    async def test_parse_valid_html_returns_structured_data(self) -> None:
        parser = HTMLParser()

        result = await parser.parse_html(VALID_HTML, BASE_URL)

        self.assertEqual(result["url"], BASE_URL)
        self.assertEqual(result["title"], "Test page")
        self.assertIn("Useful article text", result["text"])
        self.assertEqual(result["text_length"], len(result["text"]))
        self.assertEqual(result["links_count"], 3)
        self.assertEqual(result["images_count"], 2)
        self.assertEqual(
            result["metadata"],
            {
                "title": "Test page",
                "description": "Page description",
                "keywords": "python, asyncio",
            },
        )

    async def test_broken_html_does_not_break_parser(self) -> None:
        parser = HTMLParser()
        broken_html = """
        <html><head><title>Broken page</title></head>
        <body><h1>Heading<p>Text<a href="/next">Next
        """

        result = await parser.parse_html(broken_html, BASE_URL)

        self.assertEqual(result["title"], "Broken page")
        self.assertIn("https://example.test/next", result["links"])
        self.assertTrue(result["headings"])

    async def test_empty_html_returns_complete_empty_result(self) -> None:
        parser = HTMLParser()

        with self.assertLogs("crawler", level="WARNING") as logs:
            result = await parser.parse_html("", BASE_URL)

        self.assertEqual(
            result,
            {
                "url": BASE_URL,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {},
                "images": [],
                "headings": [],
                "tables": [],
                "lists": [],
                "text_length": 0,
                "links_count": 0,
                "images_count": 0,
            },
        )
        self.assertIn(BASE_URL, "\n".join(logs.output))

    async def test_relative_links_are_absolute_and_invalid_links_are_ignored(
        self,
    ) -> None:
        parser = HTMLParser()
        html = """
        <a href="/about">About</a>
        <a href="/about">Duplicate</a>
        <a href="contact">Contact</a>
        <a href="//external.test/path#section">External</a>
        <a href="#fragment">Fragment</a>
        <a href="mailto:user@example.test">Email</a>
        <a href="javascript:void(0)">JavaScript</a>
        <a href="data:text/plain,value">Data</a>
        <a>Missing href</a>
        """
        soup = BeautifulSoup(html, "html.parser")

        links = parser.extract_links(soup, BASE_URL)

        self.assertEqual(
            links,
            [
                "https://example.test/about",
                "https://example.test/docs/contact",
                "https://external.test/path",
            ],
        )

    async def test_origin_url_gets_canonical_trailing_slash(self) -> None:
        parser = HTMLParser()
        soup = BeautifulSoup(
            '<a href="https://external.test">External</a>',
            "html.parser",
        )

        links = parser.extract_links(soup, BASE_URL)

        self.assertEqual(links, ["https://external.test/"])

    async def test_extract_text_supports_css_selector(self) -> None:
        parser = HTMLParser()
        soup = BeautifulSoup(VALID_HTML, "html.parser")

        text = parser.extract_text(soup, ".article")
        missing = parser.extract_text(soup, ".missing")

        self.assertEqual(text, "Useful article text")
        self.assertEqual(missing, "")

    async def test_extracts_images_headings_tables_and_lists(self) -> None:
        parser = HTMLParser()

        result = await parser.parse_html(VALID_HTML, BASE_URL)

        self.assertEqual(
            result["images"],
            [
                {
                    "src": "https://example.test/images/one.png",
                    "alt": "First image",
                },
                {
                    "src": "https://example.test/docs/images/two.png",
                    "alt": "",
                },
            ],
        )
        self.assertEqual(
            result["headings"],
            [
                {"level": "h1", "text": "Main heading"},
                {"level": "h2", "text": "Second heading"},
                {"level": "h3", "text": "Third heading"},
            ],
        )
        self.assertEqual(
            result["tables"],
            [
                [
                    ["Name", "Value"],
                    ["alpha", "1"],
                ]
            ],
        )
        self.assertEqual(
            result["lists"],
            [
                {"type": "ul", "items": ["First", "Second"]},
                {"type": "ol", "items": ["One", "Two"]},
            ],
        )

    async def test_metadata_without_content_is_ignored(self) -> None:
        parser = HTMLParser()
        html = """
        <html><head>
          <title>Metadata page</title>
          <meta name="description">
          <meta name="keywords" content="one, two">
        </head><body>Text</body></html>
        """

        result = await parser.parse_html(html, BASE_URL)

        self.assertEqual(
            result["metadata"],
            {
                "title": "Metadata page",
                "keywords": "one, two",
            },
        )

    async def test_extractor_error_keeps_partial_results_and_logs_warning(
        self,
    ) -> None:
        parser = HTMLParser()

        with (
            patch.object(
                parser,
                "extract_tables",
                side_effect=ValueError("broken table"),
            ),
            self.assertLogs("crawler", level="WARNING") as logs,
        ):
            result = await parser.parse_html(VALID_HTML, BASE_URL)

        self.assertEqual(result["title"], "Test page")
        self.assertIn("https://example.test/about", result["links"])
        self.assertEqual(result["images_count"], 2)
        self.assertEqual(result["tables"], [])
        self.assertIn(BASE_URL, "\n".join(logs.output))
        self.assertTrue(
            any(message.startswith("WARNING:") for message in logs.output)
        )

    async def test_fetch_and_parse_integrates_crawler_with_parser(self) -> None:
        session = AsyncMock()

        with patch(
            "src.models.aiohttp.ClientSession",
            return_value=session,
        ) as session_constructor:
            crawler = AsyncCrawler()

        with patch.object(
            crawler,
            "fetch_url",
            new=AsyncMock(return_value=VALID_HTML),
        ) as fetch_url:
            result = await crawler.fetch_and_parse(BASE_URL)

        fetch_url.assert_awaited_once_with(BASE_URL)
        self.assertEqual(result["url"], BASE_URL)
        self.assertEqual(result["title"], "Test page")

        await crawler.close()
        session_constructor.assert_not_called()
        session.close.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
