# Async Crawler

Асинхронный веб-краулер на Python, созданный как семидневный учебный проект по `asyncio` и `aiohttp`. Он умеет параллельно загружать страницы, разбирать HTML, обходить ссылки с ограничением глубины, соблюдать `robots.txt`, повторять временно неудачные запросы, сохранять данные и формировать отчёты.

## Возможности

- асинхронный HTTP-клиент на `aiohttp.ClientSession`;
- глобальное и отдельное для домена ограничение конкурентности;
- rate limiting, минимальная задержка, jitter и `Crawl-delay`;
- проверка `robots.txt`, rate limiting и конкурентно-безопасное кэширование правил;
- очередь URL с приоритетом и ограничением глубины;
- фильтры домена, включения и исключения URL;
- защита от повторной обработки URL;
- парсинг текста, metadata, ссылок, изображений, заголовков, таблиц и списков;
- классификация сетевых, временных, постоянных и parsing-ошибок;
- автоматические повторы с экспоненциальным backoff;
- сохранение в JSON Lines, CSV и PostgreSQL;
- JSON- и HTML-отчёты со статистикой;
- логирование в консоль и файл с ротацией;
- CLI, JSON-конфигурация и отдельные демонстрации дней 1–7;
- benchmark времени, пропускной способности и памяти на 100/500/1000 страниц.

## Требования

- Python 3.11 или новее;
- PostgreSQL нужен только для соответствующего хранилища и демонстрации;
- Docker и Docker Compose опциональны.

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Или установить проект через `pyproject.toml`:

```bash
pip install -e .
```

Для PostgreSQL создайте `.env` на основе примера:

```bash
cp .env.example .env
docker compose up -d postgres
```

## Демонстрации по дням

Каждая демонстрация — отдельная асинхронная функция `demo_day_1()` … `demo_day_7()` в `src/main.py`.

```bash
python -m src.main --demo-day 1
python -m src.main --demo-day 2
python -m src.main --demo-day 3
python -m src.main --demo-day 4
python -m src.main --demo-day 5
python -m src.main --demo-day 6
python -m src.main --demo-day 7
```

Демонстрации запускают временный локальный `aiohttp`-сайт. Благодаря этому примеры задержек, HTTP 404/503, `robots.txt` и sitemap воспроизводятся без внешних сервисов.

День 6 по умолчанию показывает JSONL и CSV. Для PostgreSQL:

```bash
RUN_POSTGRES_DEMO=1 python -m src.main --demo-day 6
```

## CLI

Запуск краулера с настройками из `config.json`:

```bash
python -m src.main
```

Переопределение настроек из командной строки:

```bash
python -m src.main \
  --urls https://example.com \
  --max-pages 100 \
  --max-depth 2 \
  --rate-limit 2 \
  --respect-robots \
  --output results.json \
  --html-report report.html
```

Поддерживаемые параметры:

| Параметр | Назначение |
|---|---|
| `--urls URL [URL ...]` | Стартовые URL |
| `--max-pages N` | Максимальное количество страниц |
| `--max-depth N` | Максимальная глубина обхода |
| `--output FILE` | JSON-файл с результатами обхода в `public/uploads/` |
| `--html-report FILE` | HTML-отчёт в `public/uploads/` |
| `--config FILE` | JSON-конфигурация, по умолчанию `config.json` |
| `--respect-robots` | Включить соблюдение `robots.txt` |
| `--rate-limit RPS` | Максимальная частота запросов |
| `--demo-day 1..7` | Запустить демонстрацию выбранного дня |

## Конфигурация

Пример JSON-конфигурации:

```json
{
  "crawler": {
    "max_concurrent": 10,
    "max_concurrent_per_domain": 3,
    "max_depth": 2,
    "requests_per_second": 2.0,
    "respect_robots": true,
    "min_delay": 0.5,
    "user_agent": "MyCrawler/1.0",
    "connect_timeout": 5,
    "read_timeout": 10,
    "total_timeout": 30
  },
  "crawl": {
    "start_urls": ["https://example.com/"],
    "sitemap_urls": ["https://example.com/sitemap.xml"],
    "max_pages": 100,
    "same_domain_only": true,
    "include_patterns": [],
    "exclude_patterns": ["/private"]
  },
  "storage": {
    "type": "json",
    "filename": "crawler.jsonl"
  }
}
```

Доступные варианты `storage.type`:

### JSON Lines

```json
{
  "type": "json",
  "filename": "crawler.jsonl"
}
```

Одна страница записывается одной JSON-строкой. Такой формат не требует держать весь результат в памяти.

### CSV

```json
{
  "type": "csv",
  "filename": "crawler.csv",
  "encoding": "utf-8-sig"
}
```

Заголовки определяются по первой записи. Списки и словари сериализуются в JSON внутри CSV-ячеек.

### PostgreSQL

```json
{
  "type": "postgresql",
  "database": "crawler"
}
```

Параметры подключения читаются из переменных `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD` и `POSTGRES_DB`.

## Программный API

### AsyncCrawler

```python
import asyncio

from src.models import AsyncCrawler


async def main() -> None:
    crawler = AsyncCrawler(
        max_concurrent=5,
        max_concurrent_per_domain=3,
        max_depth=2,
        requests_per_second=2,
        respect_robots=True,
        min_delay=0.5,
        user_agent="MyBot/1.0",
    )

    try:
        results = await crawler.crawl(
            start_urls=["https://example.com/"],
            max_pages=50,
            same_domain_only=True,
        )
        print(results)
        print(crawler.get_stats())
    finally:
        await crawler.close()


asyncio.run(main())
```

Основные методы:

| Метод | Назначение |
|---|---|
| `fetch_url(url)` | Загрузить одну страницу |
| `fetch_urls(urls)` | Параллельно загрузить список URL |
| `fetch_urls_sequentially(urls)` | Последовательно загрузить список URL |
| `fetch_and_parse(url)` | Загрузить и разобрать HTML |
| `crawl(...)` | Запустить обход через очередь workers |
| `get_request_stats()` | Получить скорость, среднюю задержку и число robots-блокировок |
| `get_stats()` | Получить итоговую статистику обхода |
| `export_to_json(filename)` | Экспортировать статистику в JSON |
| `export_to_html_report(filename)` | Создать HTML-отчёт |
| `close()` | Закрыть HTTP-сессию и хранилище |

После `crawl()` доступны свойства `visited_urls`, `processed_urls` и `failed_urls`.

### AdvancedCrawler

```python
import asyncio

from src.crawler import AdvancedCrawler


async def main() -> None:
    crawler = AdvancedCrawler.from_config("config.json")

    try:
        await crawler.crawl()
        crawler.export_to_json("stats.json")
        crawler.export_to_html_report("report.html")
    finally:
        await crawler.close()


asyncio.run(main())
```

### HTMLParser

`HTMLParser.parse_html(html, url)` возвращает:

```python
{
    "url": str,
    "title": str,
    "text": str,
    "links": list[str],
    "metadata": dict,
    "images": list[dict],
    "headings": list[dict],
    "tables": list,
    "lists": list,
    "text_length": int,
    "links_count": int,
    "images_count": int,
}
```

CPU-зависимый разбор BeautifulSoup запускается через `asyncio.to_thread`, поэтому не блокирует основной event loop.

### RetryStrategy

```python
from src.exception import NetworkError, TransientError
from src.retry_strategy import RetryStrategy

retry_strategy = RetryStrategy(
    max_retries=3,
    backoff_factor=2,
    retry_on=[TransientError, NetworkError],
)
```

`crawl()` автоматически повторяет временные и сетевые ошибки. HTTP 429 получает увеличенную задержку. HTTP 401/403/404 классифицируются как постоянные и не повторяются.

Прямые вызовы `fetch_url()`, `fetch_urls()` и `fetch_and_parse()` не оборачиваются в `RetryStrategy` и могут выбросить `TransientError`, `NetworkError`, `PermanentError` или `RobotsBlockedError`.

## Формат сохраняемой страницы

```python
{
    "url": str,
    "title": str,
    "text": str,
    "links": list[str],
    "metadata": dict,
    "crawled_at": datetime,
    "status_code": int,
    "content_type": str,
}
```

Файловые хранилища, конфигурации демонстраций и отчёты создаются внутри `public/uploads/` через общую константу `GLOBAL_UPLOAD_PATH`. Ошибка записи логируется и не останавливает обработку остальных страниц.

## Логирование и отчёты

По умолчанию логирование настраивается функцией `setup_logging()`:

- вывод в консоль;
- файл `logs/crawler.log`;
- временные метки и уровень сообщения;
- ротация при размере 5 МБ;
- три резервных файла.

HTML-отчёт содержит итоговую таблицу, распределение HTTP-статусов, топ доменов, классификацию ошибок, retry-метрики и список URL с постоянными ошибками.
JSON, указанный через CLI-параметр `--output`, содержит собранные страницы. Статистика с классификацией ошибок и retry-метриками доступна через `get_stats()`, `export_to_json()` и HTML-отчёт.

## Benchmark и масштабируемость

Benchmark использует локальный сервер с задержкой 50 мс. Запустите его в первом терминале:

```bash
python -m src.server
```

Во втором терминале:

```bash
python -m src.benchmark
```

Будут выполнены:

1. сравнение последовательного синхронного клиента `urllib` с параллельным `AsyncCrawler`;
2. прогоны на 100, 500 и 1000 уникальных URL;
3. сравнение создания coroutine через `gather` и фиксированного пула workers;
4. замеры времени, `pages_per_second` и пикового потребления памяти через `tracemalloc`.

Адрес benchmark-сервера настраивается переменными:

```bash
SERVER_HOST=127.0.0.1
SERVER_PORT=8080
```

Workers обычно требуют меньше памяти на больших очередях, поскольку одновременно существует фиксированное количество задач. Именно worker-подход используется основным методом `crawl()`.

## Тестирование

Запуск всех тестов:

```bash
python -m unittest discover -s tests -v
```

Проверяется:

- загрузка, таймауты и сетевые ошибки;
- последовательная и конкурентная производительность;
- парсинг HTML и битого HTML;
- очередь, глубина, фильтры всех источников URL и защита от дубликатов;
- канонизация `http://host` в `http://host/`;
- rate limiting и `robots.txt`;
- retry для timeout/429/503 и отсутствие retry для 404;
- JSONL, CSV, кодировки и PostgreSQL;
- sitemap без URL из image/video-расширений, изоляция его ошибок, статистика, отчёты, CLI и демонстрации;
- benchmark на 100/500/1000 страниц.

## Структура проекта

```text
src/
├── benchmark.py       # замеры скорости и памяти
├── cli.py             # argparse CLI
├── config.py          # загрузка JSON-конфигурации
├── crawler.py         # AdvancedCrawler
├── limiter.py         # RateLimiter
├── main.py            # CLI и демонстрации дней 1–7
├── models.py          # AsyncCrawler
├── parser.py          # HTMLParser
├── queue.py           # CrawlerQueue
├── retry_strategy.py  # RetryStrategy
├── robots.py          # RobotsParser
├── semaphore.py       # SemaphoreManager
├── sitemap.py         # SitemapParser
├── stats.py           # CrawlerStats и отчёты
└── storage.py         # JSONL, CSV и PostgreSQL
```

Описание алгоритма обхода находится в [`docs/algorithm.md`](docs/algorithm.md).
