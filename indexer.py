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
            except Exception as exc:  # noqa: BLE001 - preserve retry behavior
                last_error = exc
                if attempt >= self.retries:
                    break
                sleep_for = self.backoff_seconds * (2 ** (attempt - 1))
                sleep_for += random.uniform(0.0, self.backoff_seconds)
                await asyncio.sleep(sleep_for)
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
            raise RuntimeError(f"HTTP {exc.code} from {url}: {error_body[:500]}") from exc
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
        self.api_key = os.environ.get("ODDSPAPI_KEY") or os.environ.get("ODDSPAPI_API_KEY")
        self.base_url = env_str("ODDSPAPI_BASE_URL", "https://api.oddspapi.io").rstrip("/")
        self.odds_path = env_str("ODDSPAPI_ODDS_PATH", "/v4/odds-by-tournaments")
        self.local_json_path = os.environ.get("ODDSPAPI_DISCOVERY_JSON")

    def source_is_world_cup_scoped(self) -> bool:
        return bool(
            self.local_json_path
            or os.environ.get("ODDSPAPI_TOURNAMENT_IDS")
            or os.environ.get("ODDSPAPI_TOURNAMENT_NAME")
            or os.environ.get("ODDSPAPI_WORLD_CUP_SCOPED")
        )

    async def fetch_markets(self) -> list[MarketRecord]:
        if self.local_json_path:
            payload = load_json(Path(self.local_json_path))
        elif not self.api_key:
            LOGGER.info("Skipping OddsPapi: ODDSPAPI_KEY or ODDSPAPI_API_KEY is not set")
            return []
        else:
            payload = await self._fetch_odds()
        rows = self._extract_events(payload)
        records = self._normalize_events(rows)
        return [m for m in records if self.keep_market(m)]

    async def _fetch_odds(self) -> Any:
        params = {
            "apiKey": self.api_key,
            "bookmakers": ",".join(self.config.odds_papi_bookmakers),
            "verbosity": int_env("ODDSPAPI_VERBOSITY", 3),
            "from": int(self.window.start.timestamp()),
            "to": int(self.window.end.timestamp()),
            "sport": env_str("ODDSPAPI_SPORT", "soccer"),
        }
        tournament_ids = env_csv("ODDSPAPI_TOURNAMENT_IDS")
        if tournament_ids:
            params["tournamentIds"] = ",".join(tournament_ids)
        tournament_name = os.environ.get("ODDSPAPI_TOURNAMENT_NAME")
        if tournament_name:
            params["tournamentName"] = tournament_name
        return await self.http.get_json(f"{self.base_url}{self.odds_path}", params=params)

    def _extract_events(self, payload: Any) -> list[Mapping[str, Any]]:
        if isinstance(payload, list):
            return only_mappings(payload)
        if not isinstance(payload, Mapping):
            return []
        for key in ("events", "fixtures", "games", "data", "results"):
            if isinstance(payload.get(key), list):
                return only_mappings(payload[key])
        return only_mappings(unwrap_list(payload))

    def _normalize_events(self, events: Sequence[Mapping[str, Any]]) -> list[MarketRecord]:
        records: list[MarketRecord] = []
        for event in events:
            event_id = clean_text_or_none(first_present(event, "id", "eventId", "fixtureId", "gameId"))
            event_name = clean_text_or_none(
                first_present(event, "name", "eventName", "fixtureName", "gameName", "title")
            ) or infer_event_name(event)
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


def env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def env_csv(name: str) -> list[str]:
    value = os.environ.get(name, "")
    return [part.strip() for part in value.split(",") if part.strip()]


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
