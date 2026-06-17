"""Asynchronous market discovery for the World Cup arbitrage signal engine.

This module fetches and normalizes market metadata from Polymarket, Kalshi,
OddsPapi-backed books, Azuro, and Dexsport. It is intentionally read-only:
execution, wallet signing, and bet placement are out of scope for this bot.

The public surface is:

    markets = await WorldCupIndexer.from_env().fetch_all()

It also includes amount-aware order book helpers so downstream signal code can
calculate VWAP/effective fill prices instead of naive top-of-book spreads.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import datetime as dt
import json
import logging
import math
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence


LOGGER = logging.getLogger("wc_arbbot.indexer")
UTC = dt.timezone.utc
_ODDSPAPI_TOURNAMENT_CACHE: dict[tuple[str, int, str], tuple[str, ...]] = {}
_ODDSPAPI_PARTICIPANT_CACHE: dict[tuple[str, int, str], dict[str, str]] = {}


class HTTPRequestError(RuntimeError):
    def __init__(self, method: str, url: str, status_code: int, body: str) -> None:
        self.method = method
        self.url = url
        self.status_code = status_code
        self.body = body
        super().__init__(f"HTTP {status_code} from {url}: {body[:500]}")


class Platform(StrEnum):
    POLYMARKET = "polymarket"
    KALSHI = "kalshi"
    ODDS_PAPI = "oddspapi"
    PINNACLE = "pinnacle"
    ONEXBET = "1xbet"
    AZURO = "azuro"
    DEXSPORT = "dexsport"


@dataclass(frozen=True)
class TimeWindow:
    start: dt.datetime
    end: dt.datetime

    @classmethod
    def next_hours(cls, hours: float, now: dt.datetime | None = None) -> "TimeWindow":
        current = ensure_utc(now or dt.datetime.now(tz=UTC))
        return cls(start=current, end=current + dt.timedelta(hours=hours))

    def contains(self, value: dt.datetime | None) -> bool:
        if value is None:
            return False
        value = ensure_utc(value)
        return self.start <= value <= self.end


@dataclass(frozen=True)
class OrderBookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class OrderBook:
    platform: Platform
    instrument_id: str
    bids: tuple[OrderBookLevel, ...] = ()
    asks: tuple[OrderBookLevel, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict)

    def vwap(self, side: str, quantity: float) -> float | None:
        """Return the volume weighted average fill price for a target quantity.

        side="buy" consumes asks from low to high. side="sell" consumes bids
        from high to low. Returns None when the book cannot fill the quantity.
        """

        if quantity <= 0:
            raise ValueError("quantity must be positive")
        levels = self.asks if side == "buy" else self.bids
        if side not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")
        remaining = quantity
        notional = 0.0
        for level in levels:
            if level.price <= 0 or level.size <= 0:
                continue
            take = min(remaining, level.size)
            notional += take * level.price
            remaining -= take
            if remaining <= 1e-12:
                return notional / quantity
        return None

    def fill_cost(self, side: str, quantity: float) -> float | None:
        price = self.vwap(side=side, quantity=quantity)
        return None if price is None else price * quantity


@dataclass(frozen=True)
class MarketOutcome:
    outcome_id: str
    name: str
    platform_outcome_id: str | None = None
    token_id: str | None = None
    decimal_odds: float | None = None
    probability: float | None = None
    bid: float | None = None
    ask: float | None = None
    size: float | None = None
    limit: float | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketRecord:
    platform: Platform
    market_id: str
    event_id: str | None
    event_name: str
    market_name: str
    market_type: str | None
    outcomes: tuple[MarketOutcome, ...]
    start_time: dt.datetime | None = None
    close_time: dt.datetime | None = None
    resolve_time: dt.datetime | None = None
    updated_at: dt.datetime | None = None
    status: str | None = None
    url: str | None = None
    volume: float | None = None
    liquidity: float | None = None
    limit: float | None = None
    parent_event_name: str | None = None
    mutually_exclusive_group_id: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)

    def event_or_resolution_time(self) -> dt.datetime | None:
        return self.start_time or self.resolve_time or self.close_time

    def asdict(self) -> dict[str, Any]:
        data = dataclasses.asdict(self)
        for key in ("start_time", "close_time", "resolve_time", "updated_at"):
            if data[key] is not None:
                data[key] = ensure_utc(data[key]).isoformat()
        data["platform"] = self.platform.value
        return data


@dataclass(frozen=True)
class IndexerConfig:
    window_hours: float = 48.0
    request_timeout_seconds: float = 20.0
    request_retries: int = 3
    request_backoff_seconds: float = 0.5
    concurrency: int = 8
    include_keywords: tuple[str, ...] = (
        "world cup",
        "fifa",
        "men's world cup",
        "mens world cup",
        "2026",
    )
    min_liquidity: float = 0.0
    platforms: tuple[Platform, ...] = (
        Platform.POLYMARKET,
        Platform.KALSHI,
        Platform.ODDS_PAPI,
        Platform.AZURO,
        Platform.DEXSPORT,
    )
    odds_papi_bookmakers: tuple[str, ...] = ("pinnacle", "1xbet")
    default_execution_quantity: float = 100.0

    @classmethod
    def from_env(cls) -> "IndexerConfig":
        platforms = env_csv("WC_ARBBOT_PLATFORMS")
        parsed_platforms = (
            tuple(Platform(p.strip().lower()) for p in platforms)
            if platforms
            else cls.platforms
        )
        bookmakers = env_csv("ODDSPAPI_BOOKMAKERS") or list(cls.odds_papi_bookmakers)
        keywords = env_csv("WC_ARBBOT_KEYWORDS") or list(cls.include_keywords)
        return cls(
            window_hours=float_env("WC_ARBBOT_WINDOW_HOURS", cls.window_hours),
            request_timeout_seconds=float_env(
                "WC_ARBBOT_HTTP_TIMEOUT_SECONDS", cls.request_timeout_seconds
            ),
            request_retries=int_env("WC_ARBBOT_HTTP_RETRIES", cls.request_retries),
            request_backoff_seconds=float_env(
                "WC_ARBBOT_HTTP_BACKOFF_SECONDS", cls.request_backoff_seconds
            ),
            concurrency=int_env("WC_ARBBOT_HTTP_CONCURRENCY", cls.concurrency),
            include_keywords=tuple(k.strip().lower() for k in keywords if k.strip()),
            min_liquidity=float_env("WC_ARBBOT_MIN_LIQUIDITY", cls.min_liquidity),
            platforms=parsed_platforms,
            odds_papi_bookmakers=tuple(b.strip().lower() for b in bookmakers if b.strip()),
            default_execution_quantity=float_env(
                "WC_ARBBOT_DEFAULT_EXECUTION_QTY", cls.default_execution_quantity
            ),
        )


class AsyncHTTPClient:
    """Small async HTTP client using only the Python standard library."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 20.0,
        retries: int = 3,
        backoff_seconds: float = 0.5,
        default_headers: Mapping[str, str] | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.default_headers = dict(default_headers or {})

    async def get_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        return await self.request_json("GET", url, params=params, headers=headers)

    async def post_json(
        self,
        url: str,
        *,
        payload: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        return await self.request_json("POST", url, json_body=payload, headers=headers)

    async def request_json(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        if params:
            url = append_query(url, params)
        body = None
        request_headers = {
            "Accept": "application/json",
            "User-Agent": "wc-arbbot/0.1",
            **self.default_headers,
            **dict(headers or {}),
        }
        if json_body is not None:
            body = json.dumps(json_body).encode("utf-8")
            request_headers["Content-Type"] = "application/json"

        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                return await asyncio.to_thread(
                    self._request_json_sync,
                    method,
                    url,
                    body,
                    request_headers,
                )
            except HTTPRequestError as exc:
                last_error = exc
                if not retryable_http_status(exc.status_code) or attempt >= self.retries:
                    break
                sleep_for = http_retry_delay(exc.body)
                if sleep_for is None:
                    sleep_for = self.backoff_seconds * (2 ** (attempt - 1))
                    sleep_for += random.uniform(0.0, self.backoff_seconds)
                await asyncio.sleep(sleep_for)
            except Exception as exc:  # noqa: BLE001 - preserve retry behavior
                last_error = exc
                if attempt >= self.retries:
                    break
                sleep_for = self.backoff_seconds * (2 ** (attempt - 1))
                sleep_for += random.uniform(0.0, self.backoff_seconds)
                await asyncio.sleep(sleep_for)
        if isinstance(last_error, HTTPRequestError):
            raise last_error
        raise RuntimeError(f"{method} {url} failed after {self.retries} attempts") from last_error

    def _request_json_sync(
        self,
        method: str,
        url: str,
        body: bytes | None,
        headers: Mapping[str, str],
    ) -> Any:
        req = urllib.request.Request(
            url=url,
            method=method,
            data=body,
            headers=dict(headers),
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise HTTPRequestError(method, url, exc.code, error_body) from exc
        text = payload.decode("utf-8", errors="replace")
        if not text:
            return None
        return json.loads(text)


class BaseIndexer:
    platform: Platform

    def __init__(
        self,
        config: IndexerConfig,
        http: AsyncHTTPClient,
        window: TimeWindow,
    ) -> None:
        self.config = config
        self.http = http
        self.window = window

    async def fetch_markets(self) -> list[MarketRecord]:
        raise NotImplementedError

    def source_is_world_cup_scoped(self) -> bool:
        return False

    def keep_market(self, market: MarketRecord) -> bool:
        haystack = " ".join(
            filter(
                None,
                [
                    market.event_name,
                    market.market_name,
                    market.market_type or "",
                    market.parent_event_name or "",
                ],
            )
        ).lower()
        if (
            self.config.include_keywords
            and not self.source_is_world_cup_scoped()
            and not any(k in haystack for k in self.config.include_keywords)
        ):
            return False
        if market.liquidity is not None and market.liquidity < self.config.min_liquidity:
            return False
        event_time = market.event_or_resolution_time()
        return self.window.contains(event_time)


class PolymarketIndexer(BaseIndexer):
    platform = Platform.POLYMARKET

    def __init__(self, config: IndexerConfig, http: AsyncHTTPClient, window: TimeWindow) -> None:
        super().__init__(config, http, window)
        self.gamma_base_url = env_str(
            "POLYMARKET_GAMMA_BASE_URL",
            "https://gamma-api.polymarket.com",
        ).rstrip("/")
        self.clob_base_url = env_str(
            "POLYMARKET_CLOB_BASE_URL",
            "https://clob.polymarket.com",
        ).rstrip("/")
        self.discovery_json_path = os.environ.get("POLYMARKET_DISCOVERY_JSON")
        self.page_limit = int_env("POLYMARKET_PAGE_LIMIT", 500)
        self.max_pages = int_env("POLYMARKET_MAX_PAGES", 20)

    def source_is_world_cup_scoped(self) -> bool:
        return bool(self.discovery_json_path or os.environ.get("POLYMARKET_WORLD_CUP_SCOPED"))

    async def fetch_markets(self) -> list[MarketRecord]:
        rows: list[Mapping[str, Any]]
        if self.discovery_json_path:
            rows = load_json_rows(Path(self.discovery_json_path))
        else:
            rows = await self._fetch_gamma_markets()
        markets = [m for row in rows if (m := self._normalize_market(row)) is not None]
        return [m for m in markets if self.keep_market(m)]

    async def _fetch_gamma_markets(self) -> list[Mapping[str, Any]]:
        rows: list[Mapping[str, Any]] = []
        for page in range(self.max_pages):
            params = {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": self.page_limit,
                "offset": page * self.page_limit,
                "order": "endDate",
                "ascending": "true",
            }
            data = await self.http.get_json(f"{self.gamma_base_url}/markets", params=params)
            batch = unwrap_list(data)
            if not batch:
                break
            rows.extend(only_mappings(batch))
            if len(batch) < self.page_limit:
                break
        return rows

    async def fetch_order_book(self, token_id: str) -> OrderBook:
        data = await self.http.get_json(f"{self.clob_base_url}/book", params={"token_id": token_id})
        return OrderBook(
            platform=Platform.POLYMARKET,
            instrument_id=token_id,
            bids=parse_levels(data.get("bids") or data.get("buy") or (), descending=True),
            asks=parse_levels(data.get("asks") or data.get("sell") or (), descending=False),
            raw=data if isinstance(data, Mapping) else {},
        )

    def _normalize_market(self, row: Mapping[str, Any]) -> MarketRecord | None:
        question = str(row.get("question") or row.get("title") or "").strip()
        if not question:
            return None
        market_id = str(row.get("id") or row.get("conditionId") or row.get("questionID") or "")
        if not market_id:
            return None

        outcome_names = parse_json_array(row.get("outcomes")) or ["Yes", "No"]
        outcome_prices = parse_json_array(row.get("outcomePrices")) or []
        token_ids = parse_json_array(row.get("clobTokenIds")) or []
        outcomes = []
        for idx, name in enumerate(outcome_names):
            price = safe_float(outcome_prices[idx] if idx < len(outcome_prices) else None)
            token_id = str(token_ids[idx]) if idx < len(token_ids) and token_ids[idx] is not None else None
            bid = safe_float(row.get("bestBid")) if idx == 0 else None
            ask = safe_float(row.get("bestAsk")) if idx == 0 else None
            outcomes.append(
                MarketOutcome(
                    outcome_id=f"{market_id}:{idx}",
                    name=str(name),
                    platform_outcome_id=token_id,
                    token_id=token_id,
                    probability=price,
                    bid=bid,
                    ask=ask,
                    raw={"index": idx},
                )
            )

        parent = clean_text_or_none(row.get("parent_event_title") or row.get("eventTitle"))
        start_time = parse_datetime(row.get("gameStartTime") or row.get("startTime"))
        resolve_time = parse_datetime(row.get("endDate") or row.get("endDateIso"))
        if start_time is None:
            start_time = resolve_time
        return MarketRecord(
            platform=Platform.POLYMARKET,
            market_id=market_id,
            event_id=clean_text_or_none(row.get("eventId") or row.get("gameId")),
            event_name=parent or question,
            parent_event_name=parent,
            market_name=question,
            market_type=clean_text_or_none(row.get("sportsMarketType") or row.get("marketType")),
            outcomes=tuple(outcomes),
            start_time=start_time,
            close_time=resolve_time,
            resolve_time=resolve_time,
            updated_at=parse_datetime(row.get("updatedAt")),
            status="closed" if truthy(row.get("closed")) else "active" if truthy(row.get("active")) else None,
            url=polymarket_url(row),
            volume=safe_float(row.get("volumeNum") or row.get("volume")),
            liquidity=safe_float(row.get("liquidityNum") or row.get("liquidity")),
            mutually_exclusive_group_id=clean_text_or_none(row.get("negRiskMarketID") or row.get("questionID")),
            raw=row,
        )


class KalshiIndexer(BaseIndexer):
    platform = Platform.KALSHI

    def __init__(self, config: IndexerConfig, http: AsyncHTTPClient, window: TimeWindow) -> None:
        super().__init__(config, http, window)
        self.base_url = env_str(
            "KALSHI_API_BASE_URL",
            "https://api.elections.kalshi.com/trade-api/v2",
        ).rstrip("/")
        self.discovery_json_path = os.environ.get("KALSHI_DISCOVERY_JSON")
        self.page_limit = int_env("KALSHI_PAGE_LIMIT", 1000)
        self.max_pages = int_env("KALSHI_MAX_PAGES", 20)

    def source_is_world_cup_scoped(self) -> bool:
        return bool(self.discovery_json_path or os.environ.get("KALSHI_WORLD_CUP_SCOPED"))

    async def fetch_markets(self) -> list[MarketRecord]:
        if self.discovery_json_path:
            rows = load_json_rows(Path(self.discovery_json_path))
        else:
            rows = await self._fetch_kalshi_markets()
        markets = [m for row in rows if (m := self._normalize_market(row)) is not None]
        return [m for m in markets if self.keep_market(m)]

    async def _fetch_kalshi_markets(self) -> list[Mapping[str, Any]]:
        rows: list[Mapping[str, Any]] = []
        cursor: str | None = None
        for _ in range(self.max_pages):
            params: dict[str, Any] = {"limit": self.page_limit, "status": "active"}
            if cursor:
                params["cursor"] = cursor
            data = await self.http.get_json(f"{self.base_url}/markets", params=params, headers=kalshi_headers())
            batch = data.get("markets") if isinstance(data, Mapping) else data
            batch_rows = only_mappings(unwrap_list(batch))
            rows.extend(batch_rows)
            cursor = data.get("cursor") if isinstance(data, Mapping) else None
            if not cursor or not batch_rows:
                break
        return rows

    async def fetch_order_book(self, ticker: str, depth: int = 100) -> OrderBook:
        data = await self.http.get_json(
            f"{self.base_url}/markets/{urllib.parse.quote(ticker)}/orderbook",
            params={"depth": depth},
            headers=kalshi_headers(),
        )
        book = data.get("orderbook", data) if isinstance(data, Mapping) else {}
        yes = book.get("yes") if isinstance(book, Mapping) else None
        no = book.get("no") if isinstance(book, Mapping) else None
        bids = parse_kalshi_side(yes, side="bid")
        asks = parse_kalshi_side(no, side="ask_from_no_bid")
        return OrderBook(
            platform=Platform.KALSHI,
            instrument_id=ticker,
            bids=bids,
            asks=asks,
            raw=data if isinstance(data, Mapping) else {},
        )

    def _normalize_market(self, row: Mapping[str, Any]) -> MarketRecord | None:
        ticker = clean_text_or_none(row.get("ticker"))
        title = clean_text_or_none(row.get("title"))
        if not ticker or not title:
            return None
        subtitle = clean_text_or_none(row.get("subtitle") or row.get("yes_sub_title"))
        market_name = title if not subtitle else f"{title} | {subtitle}"
        yes_ask = safe_float(row.get("yes_ask_dollars"))
        yes_bid = safe_float(row.get("yes_bid_dollars"))
        no_ask = safe_float(row.get("no_ask_dollars"))
        no_bid = safe_float(row.get("no_bid_dollars"))
        outcomes = (
            MarketOutcome(
                outcome_id=f"{ticker}:yes",
                name=clean_text_or_none(row.get("yes_sub_title")) or "Yes",
                platform_outcome_id="yes",
                probability=yes_ask,
                bid=yes_bid,
                ask=yes_ask,
                size=safe_float(row.get("yes_ask_size_fp")),
                raw={"side": "yes"},
            ),
            MarketOutcome(
                outcome_id=f"{ticker}:no",
                name=clean_text_or_none(row.get("no_sub_title")) or "No",
                platform_outcome_id="no",
                probability=no_ask,
                bid=no_bid,
                ask=no_ask,
                raw={"side": "no"},
            ),
        )
        return MarketRecord(
            platform=Platform.KALSHI,
            market_id=ticker,
            event_id=clean_text_or_none(row.get("event_ticker")),
            event_name=clean_text_or_none(row.get("parent_event_title")) or title,
            parent_event_name=clean_text_or_none(row.get("parent_event_title")),
            market_name=market_name,
            market_type=clean_text_or_none(row.get("market_type")),
            outcomes=outcomes,
            start_time=parse_datetime(row.get("occurrence_datetime")),
            close_time=parse_datetime(row.get("close_time")),
            resolve_time=parse_datetime(row.get("expected_expiration_time") or row.get("expiration_time")),
            updated_at=parse_datetime(row.get("updated_time")),
            status=clean_text_or_none(row.get("status")),
            url=f"https://kalshi.com/markets/{ticker}",
            volume=safe_float(row.get("volume_fp")),
            liquidity=safe_float(row.get("liquidity_dollars")),
            mutually_exclusive_group_id=clean_text_or_none(row.get("event_ticker")),
            raw=row,
        )


class OddsPapiIndexer(BaseIndexer):
    platform = Platform.ODDS_PAPI

    def __init__(self, config: IndexerConfig, http: AsyncHTTPClient, window: TimeWindow) -> None:
        super().__init__(config, http, window)
        self.api_key = oddspapi_api_key()
        self.base_url = env_str("ODDSPAPI_BASE_URL", "https://api.oddspapi.io").rstrip("/")
        self.odds_path = env_str("ODDSPAPI_ODDS_PATH", "/v4/odds-by-tournaments")
        self.tournaments_path = env_str("ODDSPAPI_TOURNAMENTS_PATH", "/v4/tournaments")
        self.participants_path = env_str("ODDSPAPI_PARTICIPANTS_PATH", "/v4/participants")
        self.language = env_str("ODDSPAPI_LANGUAGE", "en")
        self.sport_id = int_env("ODDSPAPI_SPORT_ID", 10)
        self.local_json_path = os.environ.get("ODDSPAPI_DISCOVERY_JSON")

    def source_is_world_cup_scoped(self) -> bool:
        return bool(
            self.local_json_path
            or os.environ.get("ODDSPAPI_TOURNAMENT_IDS")
            or os.environ.get("ODDSPAPI_TOURNAMENT_NAME")
            or os.environ.get("ODDSPAPI_WORLD_CUP_SCOPED")
        )

    async def fetch_markets(self) -> list[MarketRecord]:
        participant_names: Mapping[str, str] = {}
        if self.local_json_path:
            payload = load_json(Path(self.local_json_path))
        elif not self.api_key:
            LOGGER.info("Skipping OddsPapi: ODDSPAPI_KEY or ODDSPAPI_API_KEY is not set")
            return []
        else:
            payload = await self._fetch_odds()
            if truthy(os.environ.get("ODDSPAPI_FETCH_PARTICIPANTS", "true")):
                participant_names = await self._fetch_participant_names()
        rows = self._extract_events(payload)
        records = self._normalize_events(rows, participant_names=participant_names)
        return [m for m in records if self.keep_market(m)]

    async def _fetch_odds(self) -> Any:
        tournament_ids = await self._resolve_tournament_ids()
        if not tournament_ids:
            LOGGER.warning(
                "Skipping OddsPapi odds fetch: no tournament IDs matched %r",
                env_str("ODDSPAPI_TOURNAMENT_SEARCH", env_str("ODDSPAPI_TOURNAMENT_NAME", "FIFA World Cup")),
            )
            return []
        merged: list[Any] = []
        bookmaker_param = env_str("ODDSPAPI_BOOKMAKER_PARAM", "bookmaker")
        request_delay = float_env("ODDSPAPI_BOOKMAKER_REQUEST_DELAY_SECONDS", 1.1)
        bookmakers = self.config.odds_papi_bookmakers or ("pinnacle",)
        call_count = 0
        empty_pairs: list[str] = []
        for bookmaker in bookmakers:
            bookmaker_events = 0
            for tournament_id in tournament_ids:
                if call_count and request_delay > 0:
                    await asyncio.sleep(request_delay)
                call_count += 1
                params = {
                    "apiKey": self.api_key,
                    bookmaker_param: bookmaker,
                    "verbosity": int_env("ODDSPAPI_VERBOSITY", 3),
                    "language": self.language,
                    "oddsFormat": env_str("ODDSPAPI_ODDS_FORMAT", "decimal"),
                    "tournamentIds": tournament_id,
                }
                try:
                    payload = await self.http.get_json(f"{self.base_url}{self.odds_path}", params=params)
                except HTTPRequestError as exc:
                    if oddspapi_fixture_not_found(exc):
                        empty_pairs.append(f"{bookmaker}:{tournament_id}")
                        continue
                    raise
                events = self._extract_events(payload)
                if events:
                    LOGGER.info(
                        "Fetched %s OddsPapi fixtures for bookmaker=%s tournamentId=%s",
                        len(events),
                        bookmaker,
                        tournament_id,
                    )
                    bookmaker_events += len(events)
                    merged.extend(events)
            if bookmaker_events == 0:
                LOGGER.warning(
                    "No OddsPapi fixtures found for bookmaker=%s across tournament IDs %s",
                    bookmaker,
                    ",".join(tournament_ids),
                )
        if empty_pairs:
            LOGGER.info("OddsPapi fixture-not-found pairs skipped: %s", ", ".join(empty_pairs[:12]))
        if not merged:
            LOGGER.warning(
                "OddsPapi returned no fixtures for bookmakers=%s and tournament IDs=%s",
                ",".join(bookmakers),
                ",".join(tournament_ids),
            )
        return merged

    async def _resolve_tournament_ids(self) -> tuple[str, ...]:
        explicit_ids = env_csv("ODDSPAPI_TOURNAMENT_IDS")
        singular_id = clean_text_or_none(os.environ.get("ODDSPAPI_TOURNAMENT_ID"))
        if singular_id:
            explicit_ids.append(singular_id)
        if explicit_ids:
            return tuple(dict.fromkeys(explicit_ids))

        search = env_str("ODDSPAPI_TOURNAMENT_SEARCH", env_str("ODDSPAPI_TOURNAMENT_NAME", "FIFA World Cup"))
        cache_key = (self.base_url, self.sport_id, clean_key(search))
        if cache_key in _ODDSPAPI_TOURNAMENT_CACHE:
            return _ODDSPAPI_TOURNAMENT_CACHE[cache_key]

        if not self.api_key:
            return ()
        params = {
            "apiKey": self.api_key,
            "sportId": self.sport_id,
            "language": self.language,
        }
        payload = await self.http.get_json(f"{self.base_url}{self.tournaments_path}", params=params)
        tournament_ids = self._select_tournament_ids(payload, search)
        _ODDSPAPI_TOURNAMENT_CACHE[cache_key] = tournament_ids
        return tournament_ids

    def _select_tournament_ids(self, payload: Any, search: str) -> tuple[str, ...]:
        rows = only_mappings(unwrap_list(payload))
        exact = self._exact_tournament_matches(rows, search)
        terms = env_csv("ODDSPAPI_TOURNAMENT_MATCH_TERMS") or [
            search,
            "fifa world cup",
            "world cup",
        ]
        excludes = env_csv("ODDSPAPI_TOURNAMENT_EXCLUDE_TERMS") or [
            "women",
            "woman",
            "club",
            "qualifier",
            "qualification",
            "u17",
            "u 17",
            "u20",
            "u 20",
            "u21",
            "u 21",
            "beach",
            "futsal",
            "virtual",
            "esoccer",
            "e soccer",
            "efootball",
            "e football",
        ]
        scored: list[tuple[float, str, str]] = []
        for row in rows:
            tournament_id = clean_text_or_none(first_present(row, "tournamentId", "id"))
            if not tournament_id:
                continue
            name = clean_text_or_none(first_present(row, "tournamentName", "name", "title")) or tournament_id
            haystack = clean_key(
                " ".join(
                    str(part)
                    for part in (
                        name,
                        row.get("tournamentSlug"),
                        row.get("categoryName"),
                        row.get("categorySlug"),
                    )
                    if part
                )
            )
            if any(clean_key(term) in haystack for term in excludes if clean_key(term)):
                continue
            score = 0.0
            name_key = clean_key(name)
            for term in terms:
                term_key = clean_key(term)
                if not term_key:
                    continue
                if name_key == term_key:
                    score += 100.0
                elif term_key in haystack:
                    score += 50.0 + min(len(term_key), 25)
            activity = sum(
                safe_float(row.get(key)) or 0.0
                for key in ("liveFixtures", "upcomingFixtures", "futureFixtures")
            )
            if activity:
                score += min(activity, 20.0)
            if score > 0:
                scored.append((score, tournament_id, name))
        scored.sort(reverse=True, key=lambda item: item[0])
        limit = max(1, int_env("ODDSPAPI_MAX_TOURNAMENT_IDS", 3))
        selected_list: list[str] = []
        selected_names: dict[str, str] = {}
        if exact:
            selected_list.append(exact[0])
            selected_names[exact[0]] = exact[1]
            if truthy(os.environ.get("ODDSPAPI_STRICT_EXACT_TOURNAMENT", "false")):
                LOGGER.info(
                    "Resolved exact OddsPapi tournament ID %s for %r (%s)",
                    exact[0],
                    search,
                    exact[1],
                )
                return (exact[0],)
        for _, tournament_id, name in scored:
            if tournament_id in selected_list:
                continue
            selected_list.append(tournament_id)
            selected_names[tournament_id] = name
            if len(selected_list) >= limit:
                break
        selected = tuple(selected_list[:limit])
        if selected:
            LOGGER.info(
                "Resolved OddsPapi tournament ID candidates %s for %r (%s)",
                ",".join(selected),
                search,
                "; ".join(f"{selected_names.get(tid, tid)}:{tid}" for tid in selected),
            )
        else:
            sample = ", ".join(
                clean_text_or_none(first_present(row, "tournamentName", "name", "title")) or "unknown"
                for row in rows[:10]
            )
            LOGGER.warning("No OddsPapi tournament matched %r. Sample tournaments: %s", search, sample)
        return selected

    def _exact_tournament_matches(
        self,
        rows: Sequence[Mapping[str, Any]],
        search: str,
    ) -> tuple[str, str] | None:
        search_key = clean_key(search)
        if not search_key:
            return None
        for row in rows:
            tournament_id = clean_text_or_none(first_present(row, "tournamentId", "id"))
            name = clean_text_or_none(first_present(row, "tournamentName", "name", "title"))
            if tournament_id and name and clean_key(name) == search_key:
                return tournament_id, name
        return None

    async def _fetch_participant_names(self) -> Mapping[str, str]:
        cache_key = (self.base_url, self.sport_id, self.language)
        if cache_key in _ODDSPAPI_PARTICIPANT_CACHE:
            return _ODDSPAPI_PARTICIPANT_CACHE[cache_key]
        if not self.api_key:
            return {}
        params = {
            "apiKey": self.api_key,
            "sportId": self.sport_id,
            "language": self.language,
        }
        try:
            payload = await self.http.get_json(f"{self.base_url}{self.participants_path}", params=params)
        except Exception as exc:  # noqa: BLE001 - participant names are enrichment, not a hard dependency.
            LOGGER.warning("Could not fetch OddsPapi participant names: %s", exc)
            return {}
        names: dict[str, str] = {}
        if isinstance(payload, Mapping):
            for participant_id, name in payload.items():
                text = clean_text_or_none(name)
                if text:
                    names[str(participant_id)] = text
        _ODDSPAPI_PARTICIPANT_CACHE[cache_key] = names
        return names

    def _extract_events(self, payload: Any) -> list[Mapping[str, Any]]:
        if isinstance(payload, list):
            return only_mappings(payload)
        if not isinstance(payload, Mapping):
            return []
        for key in ("events", "fixtures", "games", "data", "results"):
            if isinstance(payload.get(key), list):
                return only_mappings(payload[key])
        return only_mappings(unwrap_list(payload))

    def _normalize_events(
        self,
        events: Sequence[Mapping[str, Any]],
        *,
        participant_names: Mapping[str, str] | None = None,
    ) -> list[MarketRecord]:
        records: list[MarketRecord] = []
        participant_names = participant_names or {}
        for event in events:
            if isinstance(event.get("bookmakerOdds"), Mapping):
                records.extend(self._normalize_odds_papi_event(event, participant_names))
                continue
            event_id = clean_text_or_none(first_present(event, "id", "eventId", "fixtureId", "gameId"))
            event_name = clean_text_or_none(
                first_present(event, "name", "eventName", "fixtureName", "gameName", "title")
            ) or odds_papi_event_name(event, participant_names)
            start_time = parse_datetime(
                first_present(event, "startTime", "commenceTime", "commence_time", "startsAt", "date")
            )
            books = unwrap_list(first_present(event, "bookmakers", "books", "sportsbooks", "odds"))
            for book in only_mappings(books):
                bookmaker = clean_text_or_none(
                    first_present(book, "key", "id", "name", "bookmaker", "sportsbook")
                )
                if bookmaker and bookmaker.lower() not in self.config.odds_papi_bookmakers:
                    continue
                markets = unwrap_list(first_present(book, "markets", "marketOdds", "odds"))
                for market in only_mappings(markets):
                    record = self._normalize_book_market(
                        event=event,
                        book=book,
                        market=market,
                        bookmaker=bookmaker,
                        event_id=event_id,
                        event_name=event_name,
                        start_time=start_time,
                    )
                    if record:
                        records.append(record)
        return records

    def _normalize_odds_papi_event(
        self,
        event: Mapping[str, Any],
        participant_names: Mapping[str, str],
    ) -> list[MarketRecord]:
        records: list[MarketRecord] = []
        event_id = clean_text_or_none(first_present(event, "fixtureId", "id", "eventId", "gameId"))
        event_name = odds_papi_event_name(event, participant_names)
        home_name, away_name = odds_papi_participant_names(event, participant_names)
        start_time = parse_datetime(
            first_present(event, "startTime", "commenceTime", "commence_time", "startsAt", "date")
        )
        bookmaker_odds = event.get("bookmakerOdds")
        if not isinstance(bookmaker_odds, Mapping):
            return records
        for bookmaker, book_payload in bookmaker_odds.items():
            bookmaker_key = str(bookmaker).strip().lower()
            if bookmaker_key not in self.config.odds_papi_bookmakers:
                continue
            if not isinstance(book_payload, Mapping):
                continue
            if "bookmakerIsActive" in book_payload and not truthy(book_payload.get("bookmakerIsActive")):
                continue
            if truthy(book_payload.get("suspended")):
                continue
            platform = Platform(bookmaker_key) if bookmaker_key in {"pinnacle", "1xbet"} else Platform.ODDS_PAPI
            for market_key, market in iter_keyed_mappings(book_payload.get("markets")):
                if "marketActive" in market and not truthy(market.get("marketActive")):
                    continue
                market_name = odds_papi_market_name(market_key, market)
                outcomes = self._normalize_odds_papi_market_outcomes(
                    market_key=market_key,
                    market=market,
                    home_name=home_name,
                    away_name=away_name,
                )
                if not outcomes:
                    continue
                market_id = ":".join(
                    filter(None, [platform.value, event_id, market_key or slugify(market_name)])
                )
                record = MarketRecord(
                    platform=platform,
                    market_id=market_id,
                    event_id=event_id,
                    event_name=event_name,
                    market_name=market_name,
                    market_type=odds_papi_market_type(market_key, market),
                    outcomes=tuple(outcomes),
                    start_time=start_time,
                    close_time=parse_datetime(first_present(market, "closeTime", "endsAt")),
                    resolve_time=start_time,
                    updated_at=parse_datetime(first_present(event, "updatedAt", "lastUpdatedAt", "timestamp"))
                    or parse_datetime(first_present(book_payload, "updatedAt", "lastUpdatedAt")),
                    status=clean_text_or_none(first_present(event, "statusName", "status", "state", "statusId")),
                    url=clean_text_or_none(first_present(book_payload, "fixturePath", "url", "link")),
                    liquidity=max_optional(outcome.limit for outcome in outcomes),
                    limit=max_optional(outcome.limit for outcome in outcomes),
                    parent_event_name=event_name,
                    mutually_exclusive_group_id=f"{event_id}:{market_key}" if event_id and market_key else None,
                    raw={"event": event, "book": book_payload, "market": market},
                )
                records.append(record)
        return records

    def _normalize_odds_papi_market_outcomes(
        self,
        *,
        market_key: str,
        market: Mapping[str, Any],
        home_name: str | None,
        away_name: str | None,
    ) -> list[MarketOutcome]:
        outcomes: list[MarketOutcome] = []
        for outcome_key, outcome_payload in iter_keyed_mappings(market.get("outcomes")):
            player_items = list(iter_keyed_mappings(outcome_payload.get("players")))
            if not player_items:
                player_items = [("0", outcome_payload)]
            for player_key, player_payload in player_items:
                if "active" in player_payload and not truthy(player_payload.get("active")):
                    continue
                bookmaker_outcome_id = clean_text_or_none(
                    first_present(
                        player_payload,
                        "bookmakerOutcomeId",
                        "outcomeName",
                        "name",
                        "label",
                    )
                ) or clean_text_or_none(first_present(outcome_payload, "bookmakerOutcomeId", "name", "label"))
                player_name = clean_text_or_none(first_present(player_payload, "playerName", "participantName"))
                outcome_name = odds_papi_outcome_name(bookmaker_outcome_id or outcome_key, home_name, away_name)
                if player_name:
                    outcome_name = f"{player_name} {outcome_name}"
                decimal_odds = safe_float(
                    first_present(player_payload, "price", "decimal", "decimalOdds", "odds", "value")
                )
                if not decimal_odds or decimal_odds <= 1:
                    continue
                normalized_outcome_id = outcome_key if player_key in {"", "0"} else f"{outcome_key}:{player_key}"
                outcomes.append(
                    MarketOutcome(
                        outcome_id=normalized_outcome_id,
                        name=outcome_name,
                        platform_outcome_id=bookmaker_outcome_id,
                        decimal_odds=decimal_odds,
                        probability=1.0 / decimal_odds,
                        limit=safe_float(first_present(player_payload, "limit", "maxBet", "maxStake")),
                        raw={
                            "marketId": market_key,
                            "outcome": outcome_payload,
                            "player": player_payload,
                        },
                    )
                )
        return outcomes

    def _normalize_book_market(
        self,
        *,
        event: Mapping[str, Any],
        book: Mapping[str, Any],
        market: Mapping[str, Any],
        bookmaker: str | None,
        event_id: str | None,
        event_name: str,
        start_time: dt.datetime | None,
    ) -> MarketRecord | None:
        market_key = clean_text_or_none(first_present(market, "key", "id", "marketId", "name"))
        market_name = clean_text_or_none(first_present(market, "name", "marketName", "label")) or market_key
        if not market_name:
            return None
        selections = unwrap_list(first_present(market, "outcomes", "selections", "prices", "runners"))
        outcomes: list[MarketOutcome] = []
        for idx, selection in enumerate(only_mappings(selections)):
            name = clean_text_or_none(first_present(selection, "name", "label", "selection", "outcome"))
            if not name:
                continue
            decimal_odds = safe_float(
                first_present(selection, "decimal", "decimalOdds", "price", "odds", "value")
            )
            limit = safe_float(
                first_present(selection, "limit", "maxBet", "maxStake", "stakeLimit", "liquidity")
            )
            outcomes.append(
                MarketOutcome(
                    outcome_id=str(first_present(selection, "id", "selectionId", "outcomeId") or f"{idx}"),
                    name=name,
                    platform_outcome_id=clean_text_or_none(
                        first_present(selection, "id", "selectionId", "outcomeId")
                    ),
                    decimal_odds=decimal_odds,
                    probability=1.0 / decimal_odds if decimal_odds and decimal_odds > 0 else None,
                    bid=safe_float(first_present(selection, "bid", "bestBid")),
                    ask=safe_float(first_present(selection, "ask", "bestAsk")),
                    limit=limit,
                    raw=selection,
                )
            )
        if not outcomes:
            return None
        platform = Platform(bookmaker.lower()) if bookmaker and bookmaker.lower() in {"pinnacle", "1xbet"} else Platform.ODDS_PAPI
        market_id = ":".join(
            filter(None, [platform.value, event_id, market_key or slugify(market_name)])
        )
        return MarketRecord(
            platform=platform,
            market_id=market_id,
            event_id=event_id,
            event_name=event_name,
            market_name=market_name,
            market_type=market_key,
            outcomes=tuple(outcomes),
            start_time=start_time,
            close_time=parse_datetime(first_present(market, "closeTime", "endsAt")),
            resolve_time=start_time,
            updated_at=parse_datetime(
                first_present(market, "updatedAt", "lastUpdatedAt", "last_update", "timestamp")
            )
            or parse_datetime(first_present(book, "updatedAt", "lastUpdatedAt")),
            status=clean_text_or_none(first_present(market, "status", "state")),
            url=clean_text_or_none(first_present(event, "url", "link")),
            volume=safe_float(first_present(market, "volume")),
            liquidity=safe_float(first_present(market, "liquidity")),
            limit=safe_float(first_present(market, "limit", "maxBet", "maxStake")),
            parent_event_name=event_name,
            mutually_exclusive_group_id=clean_text_or_none(
                first_present(market, "groupId", "marketGroupId")
            ),
            raw={"event": event, "book": book, "market": market},
        )


class AzuroIndexer(BaseIndexer):
    platform = Platform.AZURO

    def __init__(self, config: IndexerConfig, http: AsyncHTTPClient, window: TimeWindow) -> None:
        super().__init__(config, http, window)
        self.base_url = env_str("AZURO_API_BASE_URL", "https://api.onchainfeed.org/api/v1/public").rstrip("/")
        self.games_path = env_str("AZURO_GAMES_PATH", "/gateway/feed/games")
        self.graphql_url = os.environ.get("AZURO_GRAPHQL_URL")
        self.local_json_path = os.environ.get("AZURO_DISCOVERY_JSON")
        self.environment = os.environ.get("AZURO_ENVIRONMENT")

    def source_is_world_cup_scoped(self) -> bool:
        return bool(self.local_json_path or os.environ.get("AZURO_WORLD_CUP_SCOPED"))

    async def fetch_markets(self) -> list[MarketRecord]:
        if self.local_json_path:
            payload = load_json(Path(self.local_json_path))
        elif self.graphql_url:
            payload = await self._fetch_graphql()
        else:
            payload = await self._fetch_backend()
        records = self._normalize_payload(payload)
        return [m for m in records if self.keep_market(m)]

    async def _fetch_backend(self) -> Any:
        params: dict[str, Any] = {
            "startsAtGt": int(self.window.start.timestamp()),
            "startsAtLt": int(self.window.end.timestamp()),
        }
        if self.environment:
            params["environment"] = self.environment
        return await self.http.get_json(f"{self.base_url}{self.games_path}", params=params)

    async def _fetch_graphql(self) -> Any:
        query = """
        query Games($where: Game_filter!) {
          games(first: 1000, where: $where) {
            id
            gameId
            slug
            title
            startsAt
            sport { name slug }
            league { name slug }
            participants { name image }
            conditions {
              id
              conditionId
              status
              outcomes {
                id
                outcomeId
                name
                currentOdds
              }
            }
          }
        }
        """
        variables = {
            "where": {
                "startsAt_gt": int(self.window.start.timestamp()),
                "startsAt_lt": int(self.window.end.timestamp()),
                "hasActiveConditions": True,
            }
        }
        return await self.http.post_json(
            self.graphql_url,
            payload={"query": query, "variables": variables},
        )

    def _normalize_payload(self, payload: Any) -> list[MarketRecord]:
        games = extract_deep_list(payload, ("games", "data", "results", "items"))
        records: list[MarketRecord] = []
        for game in only_mappings(games):
            event_id = clean_text_or_none(first_present(game, "gameId", "id", "slug"))
            event_name = clean_text_or_none(first_present(game, "title", "name", "slug")) or infer_event_name(game)
            start_time = parse_datetime(first_present(game, "startsAt", "startTime", "date"))
            conditions = unwrap_list(first_present(game, "conditions", "markets"))
            for condition in only_mappings(conditions):
                condition_id = clean_text_or_none(first_present(condition, "conditionId", "id"))
                market_name = clean_text_or_none(
                    first_present(condition, "title", "name", "marketName")
                ) or event_name
                outcomes = []
                for idx, outcome in enumerate(only_mappings(unwrap_list(condition.get("outcomes")))):
                    name = clean_text_or_none(first_present(outcome, "name", "title", "label")) or str(idx)
                    decimal_odds = safe_float(first_present(outcome, "currentOdds", "odds", "decimalOdds"))
                    outcomes.append(
                        MarketOutcome(
                            outcome_id=str(first_present(outcome, "outcomeId", "id") or idx),
                            name=name,
                            platform_outcome_id=clean_text_or_none(
                                first_present(outcome, "outcomeId", "id")
                            ),
                            decimal_odds=decimal_odds,
                            probability=1.0 / decimal_odds if decimal_odds and decimal_odds > 0 else None,
                            limit=safe_float(first_present(outcome, "limit", "maxBet")),
                            raw=outcome,
                        )
                    )
                if not outcomes or not condition_id:
                    continue
                records.append(
                    MarketRecord(
                        platform=Platform.AZURO,
                        market_id=condition_id,
                        event_id=event_id,
                        event_name=event_name,
                        market_name=market_name,
                        market_type=clean_text_or_none(first_present(condition, "marketType", "type")),
                        outcomes=tuple(outcomes),
                        start_time=start_time,
                        close_time=parse_datetime(first_present(condition, "closeTime", "endsAt")),
                        resolve_time=start_time,
                        updated_at=parse_datetime(first_present(condition, "updatedAt")),
                        status=clean_text_or_none(first_present(condition, "status", "state")),
                        url=clean_text_or_none(first_present(game, "url", "link")),
                        volume=safe_float(first_present(condition, "volume")),
                        liquidity=safe_float(first_present(condition, "liquidity")),
                        limit=safe_float(first_present(condition, "limit", "maxBet")),
                        parent_event_name=event_name,
                        mutually_exclusive_group_id=condition_id,
                        raw={"game": game, "condition": condition},
                    )
                )
        return records


class DexsportIndexer(BaseIndexer):
    platform = Platform.DEXSPORT

    def __init__(self, config: IndexerConfig, http: AsyncHTTPClient, window: TimeWindow) -> None:
        super().__init__(config, http, window)
        self.api_url = os.environ.get("DEXSPORT_API_URL")
        self.local_json_path = os.environ.get("DEXSPORT_DISCOVERY_JSON")

    def source_is_world_cup_scoped(self) -> bool:
        return bool(self.local_json_path or os.environ.get("DEXSPORT_WORLD_CUP_SCOPED"))

    async def fetch_markets(self) -> list[MarketRecord]:
        if self.local_json_path:
            payload = load_json(Path(self.local_json_path))
        elif self.api_url:
            payload = await self.http.get_json(
                self.api_url,
                params={
                    "from": int(self.window.start.timestamp()),
                    "to": int(self.window.end.timestamp()),
                    "sport": env_str("DEXSPORT_SPORT", "soccer"),
                    "query": env_str("DEXSPORT_QUERY", "world cup"),
                },
            )
        else:
            LOGGER.info("Skipping Dexsport: set DEXSPORT_API_URL or DEXSPORT_DISCOVERY_JSON")
            return []
        return [m for m in normalize_generic_fixed_odds(payload, Platform.DEXSPORT, self.window) if self.keep_market(m)]


class WorldCupIndexer:
    def __init__(
        self,
        config: IndexerConfig,
        *,
        http: AsyncHTTPClient | None = None,
        window: TimeWindow | None = None,
    ) -> None:
        self.config = config
        self.window = window or TimeWindow.next_hours(config.window_hours)
        self.http = http or AsyncHTTPClient(
            timeout_seconds=config.request_timeout_seconds,
            retries=config.request_retries,
            backoff_seconds=config.request_backoff_seconds,
        )

    @classmethod
    def from_env(cls) -> "WorldCupIndexer":
        return cls(IndexerConfig.from_env())

    def platform_indexers(self) -> list[BaseIndexer]:
        indexers: list[BaseIndexer] = []
        if Platform.POLYMARKET in self.config.platforms:
            indexers.append(PolymarketIndexer(self.config, self.http, self.window))
        if Platform.KALSHI in self.config.platforms:
            indexers.append(KalshiIndexer(self.config, self.http, self.window))
        if Platform.ODDS_PAPI in self.config.platforms:
            indexers.append(OddsPapiIndexer(self.config, self.http, self.window))
        if Platform.AZURO in self.config.platforms:
            indexers.append(AzuroIndexer(self.config, self.http, self.window))
        if Platform.DEXSPORT in self.config.platforms:
            indexers.append(DexsportIndexer(self.config, self.http, self.window))
        return indexers

    async def fetch_all(self) -> list[MarketRecord]:
        tasks = [self._safe_fetch(indexer) for indexer in self.platform_indexers()]
        nested = await asyncio.gather(*tasks)
        markets = [market for batch in nested for market in batch]
        markets.sort(key=lambda m: (m.event_or_resolution_time() or dt.datetime.max.replace(tzinfo=UTC), m.platform.value, m.market_id))
        return markets

    async def _safe_fetch(self, indexer: BaseIndexer) -> list[MarketRecord]:
        started = time.perf_counter()
        try:
            markets = await indexer.fetch_markets()
            LOGGER.info(
                "Fetched %s %s markets in %.2fs",
                len(markets),
                indexer.platform.value,
                time.perf_counter() - started,
            )
            return markets
        except HTTPRequestError as exc:
            LOGGER.warning(
                "Failed to fetch %s markets: HTTP %s %s",
                indexer.platform.value,
                exc.status_code,
                exc.body[:250],
            )
            return []
        except Exception:
            LOGGER.exception("Failed to fetch %s markets", indexer.platform.value)
            return []


def normalize_generic_fixed_odds(
    payload: Any,
    platform: Platform,
    window: TimeWindow,
) -> list[MarketRecord]:
    events = extract_deep_list(payload, ("events", "fixtures", "games", "data", "results", "items"))
    records: list[MarketRecord] = []
    for event in only_mappings(events):
        event_id = clean_text_or_none(first_present(event, "id", "eventId", "fixtureId", "gameId"))
        event_name = clean_text_or_none(first_present(event, "name", "title", "eventName")) or infer_event_name(event)
        start_time = parse_datetime(first_present(event, "startTime", "startsAt", "commenceTime", "date"))
        if not window.contains(start_time):
            continue
        markets = unwrap_list(first_present(event, "markets", "odds", "marketOdds"))
        for market in only_mappings(markets):
            market_id = clean_text_or_none(first_present(market, "id", "marketId", "key")) or slugify(str(market))
            market_name = clean_text_or_none(first_present(market, "name", "title", "marketName", "key"))
            outcomes: list[MarketOutcome] = []
            selections = unwrap_list(first_present(market, "outcomes", "selections", "prices"))
            for idx, selection in enumerate(only_mappings(selections)):
                name = clean_text_or_none(first_present(selection, "name", "title", "label", "outcome"))
                if not name:
                    continue
                decimal_odds = safe_float(first_present(selection, "decimalOdds", "decimal", "odds", "price"))
                outcomes.append(
                    MarketOutcome(
                        outcome_id=str(first_present(selection, "id", "selectionId", "outcomeId") or idx),
                        name=name,
                        platform_outcome_id=clean_text_or_none(
                            first_present(selection, "id", "selectionId", "outcomeId")
                        ),
                        decimal_odds=decimal_odds,
                        probability=1.0 / decimal_odds if decimal_odds and decimal_odds > 0 else None,
                        limit=safe_float(first_present(selection, "limit", "maxBet", "maxStake")),
                        raw=selection,
                    )
                )
            if outcomes:
                records.append(
                    MarketRecord(
                        platform=platform,
                        market_id=f"{platform.value}:{event_id or ''}:{market_id}",
                        event_id=event_id,
                        event_name=event_name,
                        market_name=market_name or market_id,
                        market_type=clean_text_or_none(first_present(market, "type", "key")),
                        outcomes=tuple(outcomes),
                        start_time=start_time,
                        close_time=parse_datetime(first_present(market, "closeTime", "endsAt")),
                        resolve_time=start_time,
                        updated_at=parse_datetime(first_present(market, "updatedAt", "lastUpdatedAt")),
                        status=clean_text_or_none(first_present(market, "status", "state")),
                        url=clean_text_or_none(first_present(event, "url", "link")),
                        volume=safe_float(first_present(market, "volume")),
                        liquidity=safe_float(first_present(market, "liquidity")),
                        limit=safe_float(first_present(market, "limit", "maxBet", "maxStake")),
                        parent_event_name=event_name,
                        mutually_exclusive_group_id=clean_text_or_none(
                            first_present(market, "groupId", "marketGroupId")
                        ),
                        raw={"event": event, "market": market},
                    )
                )
    return records


def parse_levels(raw_levels: Any, *, descending: bool) -> tuple[OrderBookLevel, ...]:
    levels: list[OrderBookLevel] = []
    for level in unwrap_list(raw_levels):
        price: Any = None
        size: Any = None
        if isinstance(level, Mapping):
            price = first_present(level, "price", "p")
            size = first_present(level, "size", "quantity", "q")
        elif isinstance(level, Sequence) and not isinstance(level, (str, bytes)) and len(level) >= 2:
            price, size = level[0], level[1]
        parsed_price = safe_float(price)
        parsed_size = safe_float(size)
        if parsed_price is not None and parsed_size is not None:
            levels.append(OrderBookLevel(price=parsed_price, size=parsed_size))
    levels.sort(key=lambda item: item.price, reverse=descending)
    return tuple(levels)


def parse_kalshi_side(raw_levels: Any, *, side: str) -> tuple[OrderBookLevel, ...]:
    levels: list[OrderBookLevel] = []
    for level in unwrap_list(raw_levels):
        price = None
        size = None
        if isinstance(level, Mapping):
            price = first_present(level, "price", "yes_price", "no_price")
            size = first_present(level, "size", "quantity", "contracts")
        elif isinstance(level, Sequence) and not isinstance(level, (str, bytes)) and len(level) >= 2:
            price, size = level[0], level[1]
        p = safe_float(price)
        s = safe_float(size)
        if p is None or s is None:
            continue
        if p > 1.0:
            p = p / 100.0
        if side == "ask_from_no_bid":
            p = 1.0 - p
        levels.append(OrderBookLevel(price=p, size=s))
    descending = side == "bid"
    levels.sort(key=lambda item: item.price, reverse=descending)
    return tuple(levels)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_json_rows(path: Path) -> list[Mapping[str, Any]]:
    return only_mappings(unwrap_list(load_json(path)))


def unwrap_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, Mapping):
        for key in ("data", "results", "items", "events", "markets", "games", "fixtures"):
            if isinstance(value.get(key), list):
                return value[key]
        return [value]
    return []


def only_mappings(values: Iterable[Any]) -> list[Mapping[str, Any]]:
    return [v for v in values if isinstance(v, Mapping)]


def iter_keyed_mappings(value: Any) -> Iterable[tuple[str, Mapping[str, Any]]]:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(item, Mapping):
                yield str(key), item
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for idx, item in enumerate(value):
            if isinstance(item, Mapping):
                key = clean_text_or_none(first_present(item, "id", "key", "marketId", "outcomeId")) or str(idx)
                yield key, item


def max_optional(values: Iterable[float | None]) -> float | None:
    parsed = [value for value in values if value is not None]
    return max(parsed) if parsed else None


def extract_deep_list(payload: Any, keys: Sequence[str]) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, Mapping):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, Mapping):
            nested = extract_deep_list(value, keys)
            if nested:
                return nested
    data = payload.get("data")
    if isinstance(data, Mapping):
        for key in keys:
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def parse_json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def parse_datetime(value: Any) -> dt.datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        if value > 10_000_000_000:
            value = value / 1000
        return dt.datetime.fromtimestamp(value, tz=UTC)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if re.fullmatch(r"\d+(\.\d+)?", text):
        return parse_datetime(float(text))
    text = text.replace("Z", "+00:00")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}$", text):
        text = f"{text}T00:00:00+00:00"
    text = text.replace(" ", "T", 1) if " " in text and "T" not in text else text
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return ensure_utc(parsed)


def ensure_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] not in (None, ""):
            return mapping[key]
    return None


def clean_text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def clean_key(value: Any) -> str:
    text = clean_text_or_none(value) or ""
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def append_query(url: str, params: Mapping[str, Any]) -> str:
    parsed = urllib.parse.urlparse(url)
    existing = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    query = {
        **existing,
        **{k: v for k, v in params.items() if v is not None},
    }
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(query, doseq=True)))


def retryable_http_status(status_code: int) -> bool:
    return status_code == 408 or status_code == 429 or status_code >= 500


def http_retry_delay(body: str) -> float | None:
    try:
        payload = json.loads(body)
    except (TypeError, ValueError):
        return None
    error = payload.get("error") if isinstance(payload, Mapping) else None
    if not isinstance(error, Mapping):
        return None
    retry_ms = safe_float(error.get("retryMs"))
    if retry_ms is not None:
        return max(0.1, min(30.0, retry_ms / 1000.0 + 0.1))
    retry_after = clean_text_or_none(error.get("retryAfter"))
    if retry_after:
        number = safe_float(re.search(r"\d+(\.\d+)?", retry_after).group(0)) if re.search(r"\d+(\.\d+)?", retry_after) else None
        if number is not None:
            return max(0.1, min(30.0, number + 0.1))
    return None


def oddspapi_fixture_not_found(error: HTTPRequestError) -> bool:
    if error.status_code != 404:
        return False
    try:
        payload = json.loads(error.body)
    except (TypeError, ValueError):
        return False
    details = payload.get("error") if isinstance(payload, Mapping) else None
    if not isinstance(details, Mapping):
        return False
    return clean_text_or_none(details.get("code")) == "FIXTURE_NOT_FOUND"


def env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def env_csv(name: str) -> list[str]:
    value = os.environ.get(name, "")
    return [part.strip() for part in value.split(",") if part.strip()]


def oddspapi_api_key() -> str | None:
    aliases = (
        "ODDSPAPI_KEY",
        "ODDSPAPI_API_KEY",
        "ODDSPAPI_TOKEN",
        "ODDSPAPI",
        "OODSPAPI_KEY",
        "OODSPAPI_API_KEY",
        "ODDPAPI_KEY",
        "ODDS_PAPI_KEY",
        "ODDS_PAPI_API_KEY",
        "ODDS_PAPI_TOKEN",
        "ODDS_API_KEY",
        "ODDSAPI_KEY",
        "THE_ODDS_API_KEY",
        "oddspapi_key",
        "oddspapi_api_key",
        "oddspapi",
        "oodspapi_key",
        "oddpapi_key",
        "odds_papi_key",
        "oddsapi_key",
    )
    for alias in aliases:
        value = os.environ.get(alias)
        if value and value.strip():
            return value.strip()
    return None


def int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def float_env(name: str, default: float) -> float:
    value = os.environ.get(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def infer_event_name(event: Mapping[str, Any]) -> str:
    home = clean_text_or_none(first_present(event, "home", "homeTeam", "team1"))
    away = clean_text_or_none(first_present(event, "away", "awayTeam", "team2"))
    if home and away:
        return f"{home} vs. {away}"
    participants = unwrap_list(first_present(event, "participants", "competitors", "teams"))
    names = []
    for participant in only_mappings(participants):
        name = clean_text_or_none(first_present(participant, "name", "title", "team"))
        if name:
            names.append(name)
    if len(names) >= 2:
        return f"{names[0]} vs. {names[1]}"
    return clean_text_or_none(first_present(event, "slug", "id")) or "unknown event"


def odds_papi_participant_names(
    event: Mapping[str, Any],
    participant_names: Mapping[str, str],
) -> tuple[str | None, str | None]:
    participant1_id = clean_text_or_none(first_present(event, "participant1Id", "homeParticipantId", "homeId"))
    participant2_id = clean_text_or_none(first_present(event, "participant2Id", "awayParticipantId", "awayId"))
    home = clean_text_or_none(
        first_present(event, "participant1Name", "participant1ShortName", "home", "homeTeam", "team1")
    )
    away = clean_text_or_none(
        first_present(event, "participant2Name", "participant2ShortName", "away", "awayTeam", "team2")
    )
    if not home and participant1_id:
        home = participant_names.get(participant1_id)
    if not away and participant2_id:
        away = participant_names.get(participant2_id)
    return home, away


def odds_papi_event_name(event: Mapping[str, Any], participant_names: Mapping[str, str]) -> str:
    direct = clean_text_or_none(first_present(event, "name", "eventName", "fixtureName", "gameName", "title"))
    if direct:
        return direct
    home, away = odds_papi_participant_names(event, participant_names)
    if home and away:
        return f"{home} vs. {away}"
    return infer_event_name(event)


def odds_papi_market_type(market_key: str | None, market: Mapping[str, Any]) -> str | None:
    text = clean_key(
        " ".join(
            str(part)
            for part in (
                market_key,
                first_present(market, "key", "id", "marketId", "name", "marketName", "bookmakerMarketId"),
            )
            if part
        )
    )
    if "moneyline" in text or "match winner" in text or market_key == "101":
        return "moneyline"
    if "total" in text or "over under" in text:
        return "total"
    if "spread" in text or "handicap" in text:
        return "handicap"
    return clean_text_or_none(first_present(market, "key", "id", "marketId", "name", "marketName")) or market_key


def odds_papi_market_name(market_key: str | None, market: Mapping[str, Any]) -> str:
    market_type = odds_papi_market_type(market_key, market)
    if market_type == "moneyline":
        return "Moneyline"
    if market_type == "total":
        return "Total Goals"
    if market_type == "handicap":
        return "Handicap"
    return (
        clean_text_or_none(first_present(market, "name", "marketName", "label", "bookmakerMarketId"))
        or f"Market {market_key or 'unknown'}"
    )


def odds_papi_outcome_name(raw_label: str, home_name: str | None, away_name: str | None) -> str:
    label = clean_text_or_none(raw_label) or "Outcome"
    lower = label.lower().strip()
    if lower == "home":
        return home_name or "Home"
    if lower == "away":
        return away_name or "Away"
    if lower == "draw":
        return "Draw"

    parts = [part.strip() for part in re.split(r"[/|]", lower) if part.strip()]
    direction = next((part for part in parts if part in {"over", "under"}), None)
    line = next((part for part in parts if re.fullmatch(r"[+-]?\d+(\.\d+)?", part)), None)
    if direction and line:
        return f"{direction.title()} {line}"

    side = next((part for part in parts if part in {"home", "away"}), None)
    if side and line:
        team = home_name if side == "home" else away_name
        return f"{team or side.title()} {line}"

    cleaned = re.sub(r"[_/|]+", " ", label).strip()
    return re.sub(r"\s+", " ", cleaned).title()


def polymarket_url(row: Mapping[str, Any]) -> str | None:
    slug = clean_text_or_none(row.get("slug"))
    if not slug:
        return None
    return f"https://polymarket.com/event/{slug}"


def kalshi_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    key = os.environ.get("KALSHI_API_KEY") or os.environ.get("KALSHI_ACCESS_KEY")
    if key:
        headers["KALSHI-ACCESS-KEY"] = key
    token = os.environ.get("KALSHI_BEARER_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "unknown"


async def cli_async(args: argparse.Namespace) -> int:
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    config = IndexerConfig.from_env()
    if args.window_hours is not None:
        config = dataclasses.replace(config, window_hours=args.window_hours)
    if args.platforms:
        config = dataclasses.replace(
            config,
            platforms=tuple(Platform(item.strip().lower()) for item in args.platforms.split(",")),
        )
    indexer = WorldCupIndexer(config)
    markets = await indexer.fetch_all()
    payload = [market.asdict() for market in markets]
    if args.output and args.output != "-":
        Path(args.output).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        print(json.dumps(payload, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch World Cup markets for wc-arbbot.")
    parser.add_argument(
        "--output",
        "-o",
        default=os.environ.get("WC_ARBBOT_MARKETS_PATH", "markets.json"),
        help="Write normalized markets JSON to this path. Use '-' for stdout.",
    )
    parser.add_argument("--window-hours", type=float, help="Override WC_ARBBOT_WINDOW_HOURS.")
    parser.add_argument("--platforms", help="Comma-separated platform list.")
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
    )
    return parser


def main() -> int:
    return asyncio.run(cli_async(build_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
