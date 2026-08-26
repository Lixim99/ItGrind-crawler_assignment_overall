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

        self._status_codes = Counter()
        self._domains = Counter()

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

    def get_stats(self) -> dict:
        total_pages = (
            self._successful
            + self._failed
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

        return {
            "total_pages": total_pages,
            "successful": self._successful,
            "failed": self._failed,
            "average_speed": average_speed,
            "status_codes": dict(
                self._status_codes
            ),
            "top_domains": (
                self._domains.most_common(5)
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
