import json
from collections import Counter
from html import escape
from time import perf_counter
from urllib.parse import urlparse

from .constants import get_upload_path


class CrawlerStats:
    def __init__(self):
        self._started_at: float | None = None
        self._finished_at: float | None = None

        self._successful = 0
        self._failed = 0
        self._robots_blocked = 0

        self._status_codes = Counter()
        self._domains = Counter()
        self._errors_by_type = Counter()
        self._permanent_error_urls: list[str] = []
        self._robots_blocked_urls: list[str] = []
        self._retry_stats = {
            "total_retries": 0,
            "successful_after_retry": 0,
            "failed_after_retries": 0,
            "average_retry_delay": 0.0,
            "errors_by_type": {},
        }

    def start(self) -> None:
        self._started_at = perf_counter()
        self._finished_at = None

    def finish(self) -> None:
        self._finished_at = perf_counter()

    def record_success(
        self,
        url: str,
        status_code: int,
    ) -> None:
        self._successful += 1

        self._status_codes[
            status_code
        ] += 1

        domain = urlparse(url).netloc

        self._domains[
            domain
        ] += 1

    def record_failure(
        self,
        url: str,
        status_code: int | None = None,
        error_type: str | None = None,
        permanent: bool = False,
    ) -> None:
        self._failed += 1

        if status_code is not None:
            self._status_codes[
                status_code
            ] += 1

        domain = urlparse(url).netloc

        self._domains[
            domain
        ] += 1

        if error_type is not None:
            self._errors_by_type[
                error_type
            ] += 1

        if permanent and url not in self._permanent_error_urls:
            self._permanent_error_urls.append(url)

    def record_robots_blocked(
        self,
        url: str,
    ) -> None:
        self._robots_blocked += 1

        domain = urlparse(url).netloc
        self._domains[domain] += 1

        if url not in self._robots_blocked_urls:
            self._robots_blocked_urls.append(url)

    def set_retry_stats(
        self,
        retry_stats: dict,
    ) -> None:
        self._retry_stats = dict(retry_stats)

    def get_stats(self) -> dict:
        total_pages = (
            self._successful
            + self._failed
            + self._robots_blocked
        )

        if self._started_at is None:
            elapsed_time = 0.0
        else:
            end_time = (
                self._finished_at
                if self._finished_at is not None
                else perf_counter()
            )

            elapsed_time = (
                end_time
                - self._started_at
            )

        average_speed = (
            total_pages / elapsed_time
            if elapsed_time > 0
            else 0.0
        )

        errors_by_type = Counter(
            self._errors_by_type
        )
        errors_by_type.update(
            self._retry_stats.get(
                "errors_by_type",
                {},
            )
        )

        return {
            "total_pages": total_pages,
            "successful": self._successful,
            "failed": self._failed,
            "robots_blocked": self._robots_blocked,
            "average_speed": average_speed,
            "status_codes": dict(
                self._status_codes
            ),
            "top_domains": (
                self._domains.most_common(5)
            ),
            "errors_by_type": dict(
                errors_by_type
            ),
            "permanent_error_urls": list(
                self._permanent_error_urls
            ),
            "robots_blocked_urls": list(
                self._robots_blocked_urls
            ),
            "retry_stats": dict(
                self._retry_stats
            ),
            "elapsed_time": elapsed_time,
        }

    def export_to_json(
        self,
        filename: str,
    ) -> None:
        path = get_upload_path(filename)

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with path.open(
            mode="w",
            encoding="utf-8",
        ) as file:
            json.dump(
                self.get_stats(),
                file,
                ensure_ascii=False,
                indent=4,
            )

    def export_to_html_report(
        self,
        filename: str,
    ) -> None:
        stats = self.get_stats()

        status_codes = stats["status_codes"]
        top_domains = stats["top_domains"]
        errors_by_type = stats["errors_by_type"]
        permanent_error_urls = stats["permanent_error_urls"]
        robots_blocked_urls = stats["robots_blocked_urls"]
        retry_stats = stats["retry_stats"]

        max_status_count = max(
            status_codes.values(),
            default=1,
        )

        max_domain_count = max(
            (
                count
                for _, count in top_domains
            ),
            default=1,
        )

        status_rows = ""

        for status_code, count in status_codes.items():
            width = (
                count
                / max_status_count
                * 100
            )

            status_rows += f"""
              <tr>
                  <td>{status_code}</td>

                  <td>
                      <div class="bar-container">
                          <div
                              class="bar"
                              style="width: {width}%"
                          ></div>
                      </div>
                  </td>

                  <td>{count}</td>
              </tr>
          """

        domain_rows = ""

        for domain, count in top_domains:
            width = (
                count
                / max_domain_count
                * 100
            )

            safe_domain = escape(domain)

            domain_rows += f"""
              <tr>
                  <td>{safe_domain}</td>

                  <td>
                      <div class="bar-container">
                          <div
                              class="bar"
                              style="width: {width}%"
                          ></div>
                      </div>
                  </td>

                  <td>{count}</td>
              </tr>
          """

        error_rows = "".join(
            f"<tr><td>{escape(error_type)}</td><td>{count}</td></tr>"
            for error_type, count in errors_by_type.items()
        )
        permanent_error_rows = "".join(
            f"<tr><td>{escape(url)}</td></tr>"
            for url in permanent_error_urls
        )
        robots_blocked_rows = "".join(
            f"<tr><td>{escape(url)}</td></tr>"
            for url in robots_blocked_urls
        )

        html = f"""
      <!DOCTYPE html>
      <html lang="ru">
      <head>
          <meta charset="UTF-8">

          <title>Crawler report</title>

          <style>
              body {{
                  font-family: Arial, sans-serif;
                  max-width: 1000px;
                  margin: 40px auto;
                  padding: 0 20px;
              }}

              h1 {{
                  margin-bottom: 30px;
              }}

              h2 {{
                  margin-top: 30px;
              }}

              table {{
                  border-collapse: collapse;
                  width: 100%;
                  margin-bottom: 30px;
              }}

              th,
              td {{
                  border: 1px solid #ccc;
                  padding: 8px;
                  text-align: left;
              }}

              th {{
                  background: #eee;
              }}

              .bar-container {{
                  width: 100%;
                  height: 20px;
                  background: #eee;
                  border-radius: 4px;
                  overflow: hidden;
              }}

              .bar {{
                  height: 100%;
                  background: #4a90e2;
              }}
          </style>
      </head>

      <body>
          <h1>Crawler report</h1>

          <h2>Summary</h2>

          <table>
              <tr>
                  <th>Metric</th>
                  <th>Value</th>
              </tr>

              <tr>
                  <td>Total pages</td>
                  <td>{stats["total_pages"]}</td>
              </tr>

              <tr>
                  <td>Successful</td>
                  <td>{stats["successful"]}</td>
              </tr>

              <tr>
                  <td>Failed</td>
                  <td>{stats["failed"]}</td>
              </tr>

              <tr>
                  <td>Blocked by robots.txt</td>
                  <td>{stats["robots_blocked"]}</td>
              </tr>

              <tr>
                  <td>Average speed</td>
                  <td>{stats["average_speed"]:.2f} pages/sec</td>
              </tr>

              <tr>
                  <td>Elapsed time</td>
                  <td>{stats["elapsed_time"]:.2f} sec</td>
              </tr>
          </table>

          <h2>Status codes</h2>

          <table>
              <tr>
                  <th>Status</th>
                  <th>Distribution</th>
                  <th>Count</th>
              </tr>

              {status_rows}
          </table>

          <h2>Top domains</h2>

          <table>
              <tr>
                  <th>Domain</th>
                  <th>Distribution</th>
                  <th>Pages</th>
              </tr>

              {domain_rows}
          </table>

          <h2>Errors by type</h2>

          <table>
              <tr>
                  <th>Error type</th>
                  <th>Count</th>
              </tr>

              {error_rows}
          </table>

          <h2>Retry statistics</h2>

          <table>
              <tr>
                  <th>Total retries</th>
                  <th>Successful after retry</th>
                  <th>Failed after retries</th>
                  <th>Average retry delay</th>
              </tr>
              <tr>
                  <td>{retry_stats["total_retries"]}</td>
                  <td>{retry_stats["successful_after_retry"]}</td>
                  <td>{retry_stats["failed_after_retries"]}</td>
                  <td>{retry_stats["average_retry_delay"]:.2f} sec</td>
              </tr>
          </table>

          <h2>Permanent error URLs</h2>

          <table>
              <tr><th>URL</th></tr>
              {permanent_error_rows}
          </table>

          <h2>Robots.txt blocked URLs</h2>

          <table>
              <tr><th>URL</th></tr>
              {robots_blocked_rows}
          </table>
      </body>
      </html>
      """

        path = get_upload_path(filename)

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with path.open(
            mode="w",
            encoding="utf-8",
        ) as file:
            file.write(html)
