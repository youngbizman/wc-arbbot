"""Live WebSocket signalling engine for wc-arbbot.

The engine is read-only. It listens to market data streams, maintains local
L2/order quote state, computes fee-adjusted VWAP arbitrage, and sends Telegram
alerts for manual execution.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import dataclasses
import datetime as dt
import hashlib
import json
import logging
import math
import os
import random
import re
import ssl
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence
from urllib.parse import quote

from indexer import (
    AsyncHTTPClient,
    IndexerConfig,
    OrderBook,
    OrderBookLevel,
    OddsPapiIndexer,
    Platform,
    TimeWindow,
    kalshi_headers,
    oddspapi_api_key,
    parse_kalshi_side,
    parse_datetime,
    safe_float,
)
from telegram_bot import TelegramBot

try:
    import websockets
except ImportError:  # pragma: no cover - runtime dependency.
    websockets = None  # type: ignore


LOGGER = logging.getLogger("wc_arbbot.signaler")
UTC = dt.timezone.utc


@dataclass(frozen=True)
class SignalerConfig:
    taxonomy_path: Path
    target_payout_usd: float = 500.0
    min_profit_pct: float = 1.5
    quote_stale_seconds: float = 5.0
    alert_cooldown_seconds: float = 120.0
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 60.0
    ws_ping_interval_seconds: float = 20.0
    ws_ping_timeout_seconds: float = 20.0
    max_ws_message_bytes: int = 8 * 1024 * 1024
    platforms: tuple[str, ...] = ("polymarket", "kalshi", "azuro", "oddspapi")
    dry_run: bool = False
    assume_multigroup_exhaustive: bool = False
    min_multi_outcomes: int = 3
    min_platforms: int = 2
    max_alerts_per_digest: int = 2
    alert_digest_seconds: float = 60.0

    @classmethod
    def from_env(cls, taxonomy_path: str | None = None) -> "SignalerConfig":
        path = taxonomy_path or os.environ.get("WC_ARBBOT_TAXONOMY_PATH") or "taxonomy.json"
        platforms = env_csv("SIGNALER_PLATFORMS") or list(cls.platforms)
        target = float_env(
            "SIGNAL_TARGET_PAYOUT_USD",
            float_env("SIGNAL_TARGET_STAKE_USD", cls.target_payout_usd),
        )
        return cls(
            taxonomy_path=Path(path),
            target_payout_usd=target,
            min_profit_pct=float_env("SIGNAL_MIN_PROFIT_PCT", cls.min_profit_pct),
            quote_stale_seconds=float_env("SIGNAL_QUOTE_STALE_SECONDS", cls.quote_stale_seconds),
            alert_cooldown_seconds=float_env(
                "SIGNAL_ALERT_COOLDOWN_SECONDS",
                cls.alert_cooldown_seconds,
            ),
            reconnect_initial_seconds=float_env(
                "SIGNAL_RECONNECT_INITIAL_SECONDS",
                cls.reconnect_initial_seconds,
            ),
            reconnect_max_seconds=float_env("SIGNAL_RECONNECT_MAX_SECONDS", cls.reconnect_max_seconds),
            ws_ping_interval_seconds=float_env(
                "SIGNAL_WS_PING_INTERVAL_SECONDS",
                cls.ws_ping_interval_seconds,
            ),
            ws_ping_timeout_seconds=float_env(
                "SIGNAL_WS_PING_TIMEOUT_SECONDS",
                cls.ws_ping_timeout_seconds,
            ),
            max_ws_message_bytes=int_env("SIGNAL_MAX_WS_MESSAGE_BYTES", cls.max_ws_message_bytes),
            platforms=tuple(platform.strip().lower() for platform in platforms),
            dry_run=bool_env("SIGNAL_DRY_RUN", cls.dry_run),
            assume_multigroup_exhaustive=bool_env(
                "SIGNAL_ASSUME_MULTIGROUP_EXHAUSTIVE",
                cls.assume_multigroup_exhaustive,
            ),
            min_multi_outcomes=int_env("SIGNAL_MIN_MULTI_OUTCOMES", cls.min_multi_outcomes),
            min_platforms=int_env("SIGNAL_MIN_PLATFORMS", cls.min_platforms),
            max_alerts_per_digest=int_env(
                "SIGNAL_MAX_ALERTS_PER_DIGEST",
                cls.max_alerts_per_digest,
            ),
            alert_digest_seconds=float_env(
                "SIGNAL_ALERT_DIGEST_SECONDS",
                cls.alert_digest_seconds,
            ),
        )


@dataclass(frozen=True)
class InstrumentRef:
    platform: str
    canonical_id: str
    market_id: str
    outcome_id: str
    outcome_key: str
    instrument_id: str
    event_name: str
    market_name: str
    event_time: str | None = None
    market_type: str | None = None
    line: float | None = None
    url: str | None = None
    limit: float | None = None

    @property
    def quote_key(self) -> str:
        return f"{self.canonical_id}|{self.platform}|{self.instrument_id}|{self.outcome_key}"


@dataclass(frozen=True)
class LiveQuote:
    ref: InstrumentRef
    updated_at: float
    orderbook: OrderBook | None = None
    decimal_odds: float | None = None
    limit: float | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)

    def cost_for_payout(self, payout: float, fee_bps: float, fixed_fee: float) -> float | None:
        if payout <= 0:
            return None
        if self.orderbook is not None:
            cost = self.orderbook.fill_cost("buy", payout)
        elif self.decimal_odds and self.decimal_odds > 1:
            stake = payout / self.decimal_odds
            max_stake = self.limit if self.limit is not None else self.ref.limit
            if max_stake is not None and stake > max_stake + 1e-9:
                return None
            cost = stake
        else:
            return None
        if cost is None:
            return None
        return cost * (1.0 + fee_bps / 10_000.0) + fixed_fee

    def effective_probability(self, payout: float, fee_bps: float, fixed_fee: float) -> float | None:
        cost = self.cost_for_payout(payout, fee_bps, fixed_fee)
        return None if cost is None else cost / payout


@dataclass(frozen=True)
class AlertLeg:
    platform: str
    outcome_key: str
    instrument_id: str
    event_name: str
    market_name: str
    event_time: str | None
    market_type: str | None
    line: float | None
    url: str | None
    stake: float
    vwap: float
    effective_odds: float
    updated_at: float


@dataclass(frozen=True)
class ArbitrageOpportunity:
    canonical_id: str
    event_name: str
    market_name: str
    event_time: str | None
    market_type: str | None
    line: float | None
    target_payout: float
    total_cost: float
    profit: float
    profit_pct: float
    max_data_age_seconds: float
    legs: tuple[AlertLeg, ...]


class MutableOrderBook:
    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}

    def replace(self, bids: Iterable[tuple[float, float]], asks: Iterable[tuple[float, float]]) -> None:
        self.bids = {price: size for price, size in bids if price > 0 and size > 0}
        self.asks = {price: size for price, size in asks if price > 0 and size > 0}

    def apply(self, side: str, price: float, size: float) -> None:
        book_side = self.bids if side == "bid" else self.asks
        if size <= 0:
            book_side.pop(price, None)
        else:
            book_side[price] = size

    def add_delta(self, side: str, price: float, delta: float) -> None:
        book_side = self.bids if side == "bid" else self.asks
        size = book_side.get(price, 0.0) + delta
        if size <= 0:
            book_side.pop(price, None)
        else:
            book_side[price] = size

    def snapshot(self, platform: str, instrument_id: str) -> OrderBook:
        return OrderBook(
            platform=Platform(platform),
            instrument_id=instrument_id,
            bids=tuple(
                OrderBookLevel(price=price, size=size)
                for price, size in sorted(self.bids.items(), reverse=True)
                if size > 0
            ),
            asks=tuple(
                OrderBookLevel(price=price, size=size)
                for price, size in sorted(self.asks.items())
                if size > 0
            ),
        )


class TaxonomyStore:
    def __init__(self, taxonomy: Mapping[str, Any]) -> None:
        self.taxonomy = taxonomy
        self.markets: Mapping[str, Mapping[str, Any]] = {
            str(key): value
            for key, value in dict(taxonomy.get("markets", {})).items()
            if isinstance(value, Mapping)
        }
        self.clusters: Mapping[str, Mapping[str, Any]] = {
            str(cluster.get("canonical_id")): cluster
            for cluster in taxonomy.get("clusters", [])
            if isinstance(cluster, Mapping) and cluster.get("canonical_id")
        }
        self.lookup: Mapping[str, str] = {
            str(key): str(value) for key, value in dict(taxonomy.get("lookup", {})).items()
        }
        self.refs_by_alias: dict[str, list[InstrumentRef]] = defaultdict(list)
        self.refs_by_platform: dict[str, list[InstrumentRef]] = defaultdict(list)
        self.refs_by_canonical: dict[str, list[InstrumentRef]] = defaultdict(list)
        self._load_refs()

    @classmethod
    def from_path(cls, path: Path) -> "TaxonomyStore":
        with path.open("r", encoding="utf-8") as handle:
            return cls(json.load(handle))

    def _load_refs(self) -> None:
        for market_key, market in self.markets.items():
            source = as_mapping(market.get("source"))
            platform = str(market.get("platform") or source.get("platform") or "").lower()
            if not platform:
                continue
            canonical_id = self.lookup.get(market_key) or self._cluster_for_market(market_key)
            if not canonical_id:
                continue
            source_outcomes = list(source.get("outcomes") or [])
            normalized_outcomes = list(market.get("outcomes") or [])
            for index, raw_outcome in enumerate(source_outcomes or normalized_outcomes):
                outcome = as_mapping(raw_outcome)
                normalized = as_mapping(normalized_outcomes[index]) if index < len(normalized_outcomes) else {}
                outcome_id = str(
                    outcome.get("outcome_id")
                    or outcome.get("outcomeId")
                    or outcome.get("id")
                    or normalized.get("outcome_id")
                    or normalized.get("outcomeId")
                    or index
                )
                outcome_key = normalize_outcome_key(
                    normalized.get("side")
                    or normalized.get("normalized_name")
                    or outcome.get("name")
                    or outcome_id
                )
                market_type_for_ref = first_non_empty(
                    str(value)
                    for value in (
                        market.get("market_type"),
                        market.get("marketType"),
                        source.get("market_type"),
                        source.get("marketType"),
                    )
                    if value
                )
                if market_type_for_ref == "moneyline" and outcome_key in {"yes", "no"}:
                    if outcome_key == "no":
                        continue
                    selection_key = moneyline_selection_from_market(
                        str(source.get("market_name") or market.get("market_name") or "")
                    )
                    if selection_key:
                        outcome_key = selection_key
                instrument_id = str(
                    outcome.get("token_id")
                    or outcome.get("tokenId")
                    or outcome.get("platform_outcome_id")
                    or outcome.get("platformOutcomeId")
                    or outcome_id
                )
                if platform == "kalshi":
                    side = outcome_key if outcome_key in {"yes", "no"} else outcome_id.lower()
                    instrument_id = f"{source.get('market_id') or market.get('market_id')}:{side}"
                event_time = first_non_empty(
                    str(value)
                    for value in (
                        source.get("start_time"),
                        source.get("startTime"),
                        market.get("start_time"),
                        market.get("startTime"),
                        source.get("resolve_time"),
                        source.get("resolveTime"),
                        market.get("resolve_time"),
                        market.get("resolveTime"),
                    )
                    if value
                )
                ref = InstrumentRef(
                    platform=platform,
                    canonical_id=canonical_id,
                    market_id=str(source.get("market_id") or market.get("market_id") or market_key),
                    outcome_id=outcome_id,
                    outcome_key=outcome_key,
                    instrument_id=instrument_id,
                    event_name=str(source.get("event_name") or market.get("event_name") or ""),
                    market_name=str(source.get("market_name") or market.get("market_name") or ""),
                    event_time=event_time,
                    market_type=market_type_for_ref,
                    line=float_or_none(market.get("line") or source.get("line")),
                    url=source.get("url") or source.get("link"),
                    limit=float_or_none(outcome.get("limit") or source.get("limit")),
                )
                for alias in self._aliases_for_ref(ref, source, outcome):
                    self.refs_by_alias[alias].append(ref)
                self.refs_by_platform[platform].append(ref)
                self.refs_by_canonical[canonical_id].append(ref)

    def _cluster_for_market(self, market_key: str) -> str | None:
        for canonical_id, cluster in self.clusters.items():
            if market_key in set(cluster.get("markets") or []):
                return canonical_id
        return None

    def _aliases_for_ref(
        self,
        ref: InstrumentRef,
        source: Mapping[str, Any],
        outcome: Mapping[str, Any],
    ) -> set[str]:
        aliases = {
            ref.instrument_id,
            ref.quote_key,
            f"{ref.platform}:{ref.market_id}",
            f"{ref.platform}:{ref.market_id}:{ref.outcome_id}",
            f"{ref.platform}:{ref.market_id}:{ref.instrument_id}",
        }
        event_id = str(source.get("event_id") or source.get("eventId") or "")
        market_type = str(source.get("market_type") or source.get("marketType") or "")
        platform_outcome_id = str(
            outcome.get("platform_outcome_id")
            or outcome.get("platformOutcomeId")
            or outcome.get("outcome_id")
            or outcome.get("id")
            or ""
        )
        if event_id:
            aliases.add(f"{ref.platform}:{event_id}:{market_type}:{ref.outcome_id}")
            aliases.add(f"{ref.platform}:{event_id}:{market_type}:{platform_outcome_id}")
            aliases.add(f"{ref.platform}:{event_id}:{ref.market_id}:{ref.outcome_id}")
        return {alias for alias in aliases if alias and alias != "None"}

    def aliases(self, *parts: Any) -> list[str]:
        raw = [str(part) for part in parts if part not in (None, "")]
        aliases = []
        for i in range(len(raw), 0, -1):
            aliases.append(":".join(raw[:i]))
        return aliases

    def cross_platform_cluster_count(self) -> int:
        count = 0
        for refs in self.refs_by_canonical.values():
            if len({ref.platform for ref in refs}) > 1:
                count += 1
        return count


class SignalEngine:
    def __init__(
        self,
        config: SignalerConfig,
        taxonomy: TaxonomyStore,
        telegram: TelegramBot | None,
    ) -> None:
        self.config = config
        self.taxonomy = taxonomy
        self.telegram = telegram
        self.quotes: dict[str, LiveQuote] = {}
        self.pending_opportunities: dict[str, ArbitrageOpportunity] = {}
        self.last_alert_at: dict[str, float] = {}
        self.lock = asyncio.Lock()

    async def update_orderbook(
        self,
        *,
        platform: str,
        instrument_id: str,
        orderbook: OrderBook,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        refs = self.taxonomy.refs_by_alias.get(instrument_id, [])
        if not refs:
            return
        async with self.lock:
            for ref in refs:
                self.quotes[ref.quote_key] = LiveQuote(
                    ref=ref,
                    updated_at=time.time(),
                    orderbook=orderbook,
                    payload=payload or {},
                )
            await self.evaluate_refs(refs)

    async def update_fixed_odds(
        self,
        *,
        aliases: Sequence[str],
        decimal_odds: float,
        limit: float | None,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        refs: list[InstrumentRef] = []
        seen: set[str] = set()
        for alias in aliases:
            for ref in self.taxonomy.refs_by_alias.get(alias, []):
                if ref.quote_key not in seen:
                    seen.add(ref.quote_key)
                    refs.append(ref)
        if not refs:
            return
        async with self.lock:
            for ref in refs:
                self.quotes[ref.quote_key] = LiveQuote(
                    ref=ref,
                    updated_at=time.time(),
                    decimal_odds=decimal_odds,
                    limit=limit,
                    payload=payload or {},
                )
            await self.evaluate_refs(refs)

    async def evaluate_refs(self, refs: Sequence[InstrumentRef]) -> None:
        canonical_ids = {ref.canonical_id for ref in refs}
        for canonical_id in canonical_ids:
            opportunity = self.evaluate_cluster(canonical_id)
            if opportunity:
                self.queue_opportunity(opportunity)

    def evaluate_cluster(self, canonical_id: str) -> ArbitrageOpportunity | None:
        now = time.time()
        quotes = [
            quote
            for quote in self.quotes.values()
            if quote.ref.canonical_id == canonical_id
            and now - quote.updated_at <= self.config.quote_stale_seconds
        ]
        if len(quotes) < 2:
            return None

        grouped: dict[str, list[LiveQuote]] = defaultdict(list)
        for quote in quotes:
            grouped[quote.ref.outcome_key].append(quote)

        outcome_keys = choose_covering_outcomes(
            grouped.keys(),
            market_type=first_non_empty(quote.ref.market_type or "" for quote in quotes),
            assume_multigroup_exhaustive=self.config.assume_multigroup_exhaustive,
            min_multi_outcomes=self.config.min_multi_outcomes,
        )
        if not outcome_keys:
            return None

        payout = self.config.target_payout_usd
        best_legs: list[tuple[LiveQuote, float, float]] = []
        for outcome_key in outcome_keys:
            best: tuple[LiveQuote, float, float] | None = None
            for quote in grouped.get(outcome_key, []):
                fee_bps = platform_fee_bps(quote.ref.platform)
                fixed_fee = platform_fixed_fee(quote.ref.platform)
                cost = quote.cost_for_payout(payout, fee_bps, fixed_fee)
                if cost is None or cost <= 0:
                    continue
                probability = cost / payout
                if best is None or probability < best[2]:
                    best = (quote, cost, probability)
            if best is None:
                return None
            best_legs.append(best)

        if len({quote.ref.platform for quote, _, _ in best_legs}) < self.config.min_platforms:
            return None

        total_cost = sum(cost for _, cost, _ in best_legs)
        profit = payout - total_cost
        if total_cost <= 0 or profit <= 0:
            return None
        profit_pct = profit / total_cost * 100.0
        if profit_pct < self.config.min_profit_pct:
            return None

        alert_key = self.alert_key(canonical_id, best_legs, profit_pct)
        last_alert = self.last_alert_at.get(alert_key)
        if last_alert and now - last_alert < self.config.alert_cooldown_seconds:
            return None

        legs = tuple(
            AlertLeg(
                platform=quote.ref.platform,
                outcome_key=quote.ref.outcome_key,
                instrument_id=quote.ref.instrument_id,
                event_name=quote.ref.event_name,
                market_name=quote.ref.market_name,
                event_time=quote.ref.event_time,
                market_type=quote.ref.market_type,
                line=quote.ref.line,
                url=quote.ref.url,
                stake=cost,
                vwap=probability,
                effective_odds=payout / cost,
                updated_at=quote.updated_at,
            )
            for quote, cost, probability in best_legs
        )
        event_name = first_non_empty(leg.event_name for leg in legs) or canonical_id
        market_name = first_non_empty(leg.market_name for leg in legs) or canonical_id
        event_time = first_non_empty(leg.event_time or "" for leg in legs)
        market_type = first_non_empty(leg.market_type or "" for leg in legs)
        line = first_non_null(leg.line for leg in legs)
        return ArbitrageOpportunity(
            canonical_id=canonical_id,
            event_name=event_name,
            market_name=market_name,
            event_time=event_time,
            market_type=market_type,
            line=line,
            target_payout=payout,
            total_cost=total_cost,
            profit=profit,
            profit_pct=profit_pct,
            max_data_age_seconds=max(now - leg.updated_at for leg in legs),
            legs=legs,
        )

    def queue_opportunity(self, opportunity: ArbitrageOpportunity) -> None:
        key = self.alert_key(
            opportunity.canonical_id,
            tuple((leg, leg.stake, leg.vwap) for leg in opportunity.legs),  # type: ignore[arg-type]
            opportunity.profit_pct,
        )
        now = time.time()
        last_alert = self.last_alert_at.get(key)
        if last_alert and now - last_alert < self.config.alert_cooldown_seconds:
            return
        current = self.pending_opportunities.get(key)
        if (
            current is None
            or opportunity_latest_update(opportunity) >= opportunity_latest_update(current)
            or (opportunity.profit, opportunity.profit_pct) > (current.profit, current.profit_pct)
        ):
            self.pending_opportunities[key] = opportunity

    async def run_digest_loop(self) -> None:
        LOGGER.info(
            "Alert digest enabled: sending top %s by net profit every %.1fs",
            self.config.max_alerts_per_digest,
            self.config.alert_digest_seconds,
        )
        while True:
            await asyncio.sleep(max(1.0, self.config.alert_digest_seconds))
            await self.flush_alert_digest()

    async def flush_alert_digest(self) -> None:
        async with self.lock:
            if not self.pending_opportunities:
                return
            opportunities = list(self.pending_opportunities.values())
            self.pending_opportunities.clear()
        limit = max(0, self.config.max_alerts_per_digest)
        if limit == 0:
            LOGGER.info("Discarded %s pending opportunities because max alerts is 0", len(opportunities))
            return
        selected = sorted(
            opportunities,
            key=lambda opportunity: (opportunity.profit, opportunity.profit_pct),
            reverse=True,
        )[:limit]
        LOGGER.warning(
            "Alert digest sending %s/%s opportunities; best net profit %s",
            len(selected),
            len(opportunities),
            f"${selected[0].profit:,.2f}" if selected else "$0.00",
        )
        for opportunity in selected:
            await self.alert(opportunity)

    async def alert(self, opportunity: ArbitrageOpportunity) -> None:
        alert_key = self.alert_key(
            opportunity.canonical_id,
            tuple((leg, leg.stake, leg.vwap) for leg in opportunity.legs),  # type: ignore[arg-type]
            opportunity.profit_pct,
        )
        self.last_alert_at[alert_key] = time.time()
        LOGGER.warning(
            "ARB %.2f%% %s %s",
            opportunity.profit_pct,
            opportunity.event_name,
            opportunity.market_name,
        )
        if self.config.dry_run or not self.telegram:
            LOGGER.info("Dry-run alert payload: %s", dataclasses.asdict(opportunity))
            return
        try:
            await self.telegram.send_alert(opportunity)
        except Exception:
            LOGGER.exception("Failed to send Telegram alert")

    def alert_key(self, canonical_id: str, legs: Sequence[Any], profit_pct: float) -> str:
        raw = json.dumps(
            {
                "canonical_id": canonical_id,
                "legs": [
                    stable_leg_id(item)
                    for item in legs
                ],
            },
            sort_keys=True,
        )
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()


class ReconnectingListener:
    name = "listener"

    def __init__(self, config: SignalerConfig, engine: SignalEngine) -> None:
        self.config = config
        self.engine = engine

    async def run_forever(self) -> None:
        delay = self.config.reconnect_initial_seconds
        while True:
            try:
                await self.run_once()
                delay = self.config.reconnect_initial_seconds
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("%s disconnected", self.name)
                sleep_for = min(delay, self.config.reconnect_max_seconds)
                sleep_for += random.uniform(0.0, min(1.0, sleep_for / 4.0))
                await asyncio.sleep(sleep_for)
                delay = min(delay * 2, self.config.reconnect_max_seconds)

    async def run_once(self) -> None:
        raise NotImplementedError

    async def connect(self, url: str, headers: Mapping[str, str] | None = None) -> Any:
        if websockets is None:
            raise RuntimeError("The 'websockets' package is required for WebSocket live signalling")
        if not url.startswith(("ws://", "wss://")):
            raise RuntimeError(f"{self.name} websocket URL must start with ws:// or wss://, got {url!r}")
        kwargs = {
            "ping_interval": self.config.ws_ping_interval_seconds,
            "ping_timeout": self.config.ws_ping_timeout_seconds,
            "max_size": self.config.max_ws_message_bytes,
            "ssl": ssl.create_default_context() if url.startswith("wss://") else None,
        }
        try:
            return await websockets.connect(url, additional_headers=dict(headers or {}), **kwargs)
        except TypeError:
            return await websockets.connect(url, extra_headers=dict(headers or {}), **kwargs)


class PolymarketListener(ReconnectingListener):
    name = "polymarket"

    def __init__(self, config: SignalerConfig, engine: SignalEngine, taxonomy: TaxonomyStore) -> None:
        super().__init__(config, engine)
        self.taxonomy = taxonomy
        self.url = env_str(
            "POLYMARKET_WS_URL",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        )
        self.chunk_size = int_env("POLYMARKET_WS_CHUNK_SIZE", 200)
        self.books: dict[str, MutableOrderBook] = defaultdict(MutableOrderBook)

    async def run_once(self) -> None:
        token_ids = sorted(
            {
                ref.instrument_id
                for ref in self.taxonomy.refs_by_platform.get("polymarket", [])
                if ref.instrument_id and ref.instrument_id.lower() not in {"yes", "no"}
            }
        )
        if not token_ids:
            LOGGER.info("Skipping Polymarket: no token IDs in taxonomy")
            await asyncio.sleep(3600)
            return
        for chunk in chunks(token_ids, self.chunk_size):
            asyncio.create_task(self._run_chunk(chunk))
        while True:
            await asyncio.sleep(3600)

    async def _run_chunk(self, token_ids: Sequence[str]) -> None:
        delay = self.config.reconnect_initial_seconds
        while True:
            try:
                ws = await self.connect(self.url)
                async with ws:
                    await ws.send(
                        json.dumps(
                            {
                                "assets_ids": list(token_ids),
                                "type": "market",
                                "custom_feature_enabled": True,
                            }
                        )
                    )
                    async for raw in ws:
                        for message in decode_ws_payload(raw):
                            await self.handle_message(message)
                delay = self.config.reconnect_initial_seconds
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("Polymarket chunk disconnected")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.config.reconnect_max_seconds)

    async def handle_message(self, message: Mapping[str, Any]) -> None:
        event_type = message.get("event_type")
        if event_type == "book":
            asset_id = str(message.get("asset_id") or "")
            if not asset_id:
                return
            bids = parse_price_size_levels(message.get("bids"))
            asks = parse_price_size_levels(message.get("asks"))
            self.books[asset_id].replace(bids, asks)
            await self.engine.update_orderbook(
                platform="polymarket",
                instrument_id=asset_id,
                orderbook=self.books[asset_id].snapshot("polymarket", asset_id),
                payload=message,
            )
        elif event_type == "price_change":
            for change in message.get("price_changes") or []:
                if not isinstance(change, Mapping):
                    continue
                asset_id = str(change.get("asset_id") or "")
                price = float_or_none(change.get("price"))
                size = float_or_none(change.get("size"))
                side_raw = str(change.get("side") or "").upper()
                if not asset_id or price is None or size is None:
                    continue
                side = "bid" if side_raw == "BUY" else "ask"
                self.books[asset_id].apply(side, price, size)
                await self.engine.update_orderbook(
                    platform="polymarket",
                    instrument_id=asset_id,
                    orderbook=self.books[asset_id].snapshot("polymarket", asset_id),
                    payload=message,
                )


class KalshiListener(ReconnectingListener):
    name = "kalshi"

    def __init__(self, config: SignalerConfig, engine: SignalEngine, taxonomy: TaxonomyStore) -> None:
        super().__init__(config, engine)
        self.taxonomy = taxonomy
        self.url = env_str(
            "KALSHI_WS_URL",
            "wss://api.elections.kalshi.com/trade-api/ws/v2",
        )
        self.path = env_str("KALSHI_WS_PATH", "/trade-api/ws/v2")
        self.command_id = 1
        self.books: dict[str, dict[str, MutableOrderBook]] = defaultdict(
            lambda: {"yes": MutableOrderBook(), "no": MutableOrderBook()}
        )

    async def run_once(self) -> None:
        tickers = sorted(
            {
                ref.market_id
                for ref in self.taxonomy.refs_by_platform.get("kalshi", [])
                if ref.market_id
            }
        )
        if not tickers:
            LOGGER.info("Skipping Kalshi: no market tickers in taxonomy")
            await asyncio.sleep(3600)
            return
        ws = await self.connect(self.url, headers=kalshi_auth_headers(self.path))
        async with ws:
            for chunk in chunks(tickers, int_env("KALSHI_WS_CHUNK_SIZE", 100)):
                await ws.send(
                    json.dumps(
                        {
                            "id": self.next_command_id(),
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["orderbook_delta"],
                                "market_tickers": list(chunk),
                            },
                        }
                    )
                )
            async for raw in ws:
                for message in decode_ws_payload(raw):
                    await self.handle_message(message)

    def next_command_id(self) -> int:
        self.command_id += 1
        return self.command_id

    async def handle_message(self, message: Mapping[str, Any]) -> None:
        msg_type = message.get("type")
        msg = as_mapping(message.get("msg"))
        ticker = str(msg.get("market_ticker") or "")
        if not ticker:
            return
        if msg_type == "orderbook_snapshot":
            yes_levels = parse_price_size_levels(msg.get("yes_dollars_fp"))
            no_levels = parse_price_size_levels(msg.get("no_dollars_fp"))
            self.books[ticker]["yes"].replace(yes_levels, ())
            self.books[ticker]["no"].replace(no_levels, ())
        elif msg_type == "orderbook_delta":
            price = float_or_none(msg.get("price_dollars"))
            delta = float_or_none(msg.get("delta_fp"))
            side = str(msg.get("side") or "").lower()
            if price is None or delta is None or side not in {"yes", "no"}:
                return
            self.books[ticker][side].add_delta("bid", price, delta)
        else:
            return
        await self.publish_ticker_books(ticker, message)

    async def publish_ticker_books(self, ticker: str, payload: Mapping[str, Any]) -> None:
        yes_bids = self.books[ticker]["yes"].bids
        no_bids = self.books[ticker]["no"].bids
        yes_book = MutableOrderBook()
        yes_book.replace(
            yes_bids.items(),
            ((1.0 - price, size) for price, size in no_bids.items()),
        )
        no_book = MutableOrderBook()
        no_book.replace(
            no_bids.items(),
            ((1.0 - price, size) for price, size in yes_bids.items()),
        )
        await self.engine.update_orderbook(
            platform="kalshi",
            instrument_id=f"{ticker}:yes",
            orderbook=yes_book.snapshot("kalshi", f"{ticker}:yes"),
            payload=payload,
        )
        await self.engine.update_orderbook(
            platform="kalshi",
            instrument_id=f"{ticker}:no",
            orderbook=no_book.snapshot("kalshi", f"{ticker}:no"),
            payload=payload,
        )


class KalshiRestPollingListener(ReconnectingListener):
    name = "kalshi"

    def __init__(self, config: SignalerConfig, engine: SignalEngine, taxonomy: TaxonomyStore) -> None:
        super().__init__(config, engine)
        self.taxonomy = taxonomy
        self.base_url = env_str(
            "KALSHI_API_BASE_URL",
            "https://api.elections.kalshi.com/trade-api/v2",
        ).rstrip("/")
        self.poll_seconds = float_env("KALSHI_REST_POLL_SECONDS", 15.0)
        self.depth = int_env("KALSHI_REST_ORDERBOOK_DEPTH", 100)
        self.batch_size = int_env("KALSHI_REST_BATCH_SIZE", 25)
        self.http = AsyncHTTPClient(
            timeout_seconds=float_env("KALSHI_REST_TIMEOUT_SECONDS", 15.0),
            retries=int_env("KALSHI_REST_RETRIES", 2),
            backoff_seconds=float_env("KALSHI_REST_BACKOFF_SECONDS", 0.3),
        )

    async def run_once(self) -> None:
        tickers = sorted(
            {
                ref.market_id
                for ref in self.taxonomy.refs_by_platform.get("kalshi", [])
                if ref.market_id
            }
        )
        if not tickers:
            LOGGER.info("Skipping Kalshi REST polling: no market tickers in taxonomy")
            await asyncio.sleep(3600)
            return
        LOGGER.info("Starting Kalshi REST polling for %s tickers every %.1fs", len(tickers), self.poll_seconds)
        while True:
            for chunk in chunks(tickers, self.batch_size):
                await asyncio.gather(
                    *(self.fetch_and_publish(ticker) for ticker in chunk),
                    return_exceptions=True,
                )
            await asyncio.sleep(max(1.0, self.poll_seconds))

    async def fetch_and_publish(self, ticker: str) -> None:
        try:
            data = await self.http.get_json(
                f"{self.base_url}/markets/{quote(ticker)}/orderbook",
                params={"depth": self.depth},
                headers=kalshi_headers(),
            )
        except Exception:
            LOGGER.debug("Kalshi REST orderbook fetch failed for %s", ticker, exc_info=True)
            return
        book = as_mapping(data.get("orderbook") if isinstance(data, Mapping) else data)
        yes = book.get("yes")
        no = book.get("no")
        yes_book = MutableOrderBook()
        yes_book.replace(
            ((level.price, level.size) for level in parse_kalshi_side(yes, side="bid")),
            ((level.price, level.size) for level in parse_kalshi_side(no, side="ask_from_no_bid")),
        )
        no_book = MutableOrderBook()
        no_book.replace(
            ((level.price, level.size) for level in parse_kalshi_side(no, side="bid")),
            ((level.price, level.size) for level in parse_kalshi_side(yes, side="ask_from_no_bid")),
        )
        await self.engine.update_orderbook(
            platform="kalshi",
            instrument_id=f"{ticker}:yes",
            orderbook=yes_book.snapshot("kalshi", f"{ticker}:yes"),
            payload=data if isinstance(data, Mapping) else {},
        )
        await self.engine.update_orderbook(
            platform="kalshi",
            instrument_id=f"{ticker}:no",
            orderbook=no_book.snapshot("kalshi", f"{ticker}:no"),
            payload=data if isinstance(data, Mapping) else {},
        )


class AzuroListener(ReconnectingListener):
    name = "azuro"

    def __init__(self, config: SignalerConfig, engine: SignalEngine, taxonomy: TaxonomyStore) -> None:
        super().__init__(config, engine)
        self.taxonomy = taxonomy
        self.url = env_str(
            "AZURO_WS_URL",
            "wss://dev-streams.onchainfeed.org/v1/streams/feed",
        )

    async def run_once(self) -> None:
        refs = self.taxonomy.refs_by_platform.get("azuro", [])
        if not refs:
            LOGGER.info("Skipping Azuro: no Azuro refs in taxonomy")
            await asyncio.sleep(3600)
            return
        ws = await self.connect(self.url)
        async with ws:
            payload = os.environ.get("AZURO_WS_SUBSCRIBE_PAYLOAD")
            if payload:
                await ws.send(payload)
            async for raw in ws:
                for message in decode_ws_payload(raw):
                    await self.handle_message(message)

    async def handle_message(self, message: Mapping[str, Any]) -> None:
        for update in extract_odds_like_updates(message):
            condition_id = update.get("conditionId") or update.get("condition_id") or update.get("marketId")
            outcome_id = update.get("outcomeId") or update.get("outcome_id") or update.get("id")
            decimal_odds = float_or_none(
                update.get("currentOdds") or update.get("odds") or update.get("decimalOdds")
            )
            if not condition_id or not outcome_id or not decimal_odds:
                continue
            aliases = [
                f"azuro:{condition_id}:{outcome_id}",
                f"azuro:{condition_id}",
                str(outcome_id),
            ]
            await self.engine.update_fixed_odds(
                aliases=aliases,
                decimal_odds=decimal_odds,
                limit=float_or_none(update.get("limit") or update.get("maxBet")),
                payload=message,
            )


class OddsPapiListener(ReconnectingListener):
    name = "oddspapi"

    def __init__(self, config: SignalerConfig, engine: SignalEngine) -> None:
        super().__init__(config, engine)
        self.api_key = oddspapi_api_key()
        base = env_str("ODDSPAPI_WS_URL", "wss://api.oddspapi.io/v4/ws")
        self.url = f"{base}?apiKey={quote(self.api_key or '')}"
        self.bookmakers = {book.lower() for book in env_csv("ODDSPAPI_BOOKMAKERS")}
        if not self.bookmakers:
            self.bookmakers = {"pinnacle", "1xbet"}

    async def run_once(self) -> None:
        if not self.api_key:
            LOGGER.info("Skipping OddsPapi: ODDSPAPI_KEY or ODDSPAPI_API_KEY is not set")
            await asyncio.sleep(3600)
            return
        ws = await self.connect(self.url)
        async with ws:
            async for raw in ws:
                for message in decode_ws_payload(raw):
                    await self.handle_message(message)

    async def handle_message(self, message: Mapping[str, Any]) -> None:
        fixture_id = message.get("fixtureId")
        bookmaker_odds = as_mapping(message.get("bookmakerOdds"))
        for bookmaker, book_payload_raw in bookmaker_odds.items():
            book = str(bookmaker).lower()
            if "all" not in self.bookmakers and book not in self.bookmakers:
                continue
            book_payload = as_mapping(book_payload_raw)
            markets = as_mapping(book_payload.get("markets"))
            for market_id, market_payload_raw in markets.items():
                market_payload = as_mapping(market_payload_raw)
                outcomes = as_mapping(market_payload.get("outcomes"))
                for outcome_id, outcome_payload_raw in outcomes.items():
                    outcome_payload = as_mapping(outcome_payload_raw)
                    players = as_mapping(outcome_payload.get("players"))
                    if players:
                        for player_id, player_payload_raw in players.items():
                            player_payload = as_mapping(player_payload_raw)
                            await self._publish_price(
                                book,
                                fixture_id,
                                market_id,
                                outcome_id,
                                player_id,
                                player_payload,
                                message,
                            )
                    else:
                        await self._publish_price(
                            book,
                            fixture_id,
                            market_id,
                            outcome_id,
                            None,
                            outcome_payload,
                            message,
                        )

    async def _publish_price(
        self,
        bookmaker: str,
        fixture_id: Any,
        market_id: Any,
        outcome_id: Any,
        player_id: Any,
        payload: Mapping[str, Any],
        original_message: Mapping[str, Any],
    ) -> None:
        price = float_or_none(payload.get("price") or payload.get("decimalOdds") or payload.get("odds"))
        if not price or price <= 1:
            return
        aliases = [
            f"{bookmaker}:{fixture_id}:{market_id}:{outcome_id}:{player_id}",
            f"{bookmaker}:{fixture_id}:{market_id}:{outcome_id}",
            f"{bookmaker}:{fixture_id}:{market_id}",
            f"{bookmaker}:{market_id}:{outcome_id}:{player_id}",
            f"{bookmaker}:{market_id}:{outcome_id}",
            str(payload.get("oddsId") or ""),
            str(payload.get("bookmakerOutcomeId") or ""),
        ]
        await self.engine.update_fixed_odds(
            aliases=[alias for alias in aliases if alias and "None" not in alias],
            decimal_odds=price,
            limit=float_or_none(payload.get("limit")),
            payload=original_message,
        )


class OddsPapiRestPollingListener(ReconnectingListener):
    name = "oddspapi"

    def __init__(self, config: SignalerConfig, engine: SignalEngine) -> None:
        super().__init__(config, engine)
        self.api_key = oddspapi_api_key()
        self.poll_seconds = float_env("ODDSPAPI_REST_POLL_SECONDS", 20.0)
        self.config_snapshot = IndexerConfig.from_env()
        self.http = AsyncHTTPClient(
            timeout_seconds=float_env("ODDSPAPI_REST_TIMEOUT_SECONDS", 20.0),
            retries=int_env("ODDSPAPI_REST_RETRIES", 2),
            backoff_seconds=float_env("ODDSPAPI_REST_BACKOFF_SECONDS", 0.5),
        )

    async def run_once(self) -> None:
        if not self.api_key:
            LOGGER.info("Skipping OddsPapi REST polling: no OddsPapi key is set")
            await asyncio.sleep(3600)
            return
        LOGGER.info(
            "Starting OddsPapi REST polling for bookmakers=%s every %.1fs",
            ",".join(self.config_snapshot.odds_papi_bookmakers),
            self.poll_seconds,
        )
        while True:
            await self.poll_once()
            await asyncio.sleep(max(5.0, self.poll_seconds))

    async def poll_once(self) -> None:
        window = TimeWindow.next_hours(float_env("WC_ARBBOT_WINDOW_HOURS", self.config_snapshot.window_hours))
        indexer = OddsPapiIndexer(self.config_snapshot, self.http, window)
        try:
            markets = await indexer.fetch_markets()
        except Exception:
            LOGGER.exception("OddsPapi REST poll failed")
            return
        updates = 0
        for market in markets:
            platform = market.platform.value if hasattr(market.platform, "value") else str(market.platform)
            for outcome in market.outcomes:
                decimal_odds = outcome.decimal_odds
                if decimal_odds is None and outcome.probability and outcome.probability > 0:
                    decimal_odds = 1.0 / outcome.probability
                if not decimal_odds or decimal_odds <= 1:
                    continue
                aliases = [
                    f"{platform}:{market.market_id}:{outcome.outcome_id}",
                    f"{platform}:{market.market_id}:{outcome.platform_outcome_id}",
                    f"{platform}:{market.event_id}:{market.market_type}:{outcome.outcome_id}",
                    f"{platform}:{market.event_id}:{market.market_type}:{outcome.platform_outcome_id}",
                    str(outcome.outcome_id or ""),
                    str(outcome.platform_outcome_id or ""),
                ]
                await self.engine.update_fixed_odds(
                    aliases=[alias for alias in aliases if alias and "None" not in alias],
                    decimal_odds=decimal_odds,
                    limit=outcome.limit or market.limit,
                    payload=market.raw,
                )
                updates += 1
        LOGGER.info("OddsPapi REST poll published %s fixed-odds updates from %s markets", updates, len(markets))


class LiveSignaler:
    def __init__(self, config: SignalerConfig) -> None:
        self.config = config
        self.taxonomy = TaxonomyStore.from_path(config.taxonomy_path)
        telegram = None if config.dry_run else TelegramBot()
        self.engine = SignalEngine(config, self.taxonomy, telegram)

    async def run(self) -> None:
        listeners: list[ReconnectingListener] = []
        enabled = set(self.config.platforms)
        if not self.taxonomy.clusters and not bool_env("SIGNAL_ALLOW_EMPTY_TAXONOMY", False):
            raise RuntimeError(
                "Taxonomy contains 0 canonical clusters. Run indexer/nlp_mapper with discovery data "
                "before starting the live signaler, or set SIGNAL_ALLOW_EMPTY_TAXONOMY=true for a dry connectivity run."
            )
        if "polymarket" in enabled:
            listeners.append(PolymarketListener(self.config, self.engine, self.taxonomy))
        if "kalshi" in enabled:
            if has_kalshi_auth():
                listeners.append(KalshiListener(self.config, self.engine, self.taxonomy))
            elif bool_env("SIGNAL_REQUIRE_KALSHI", False):
                raise RuntimeError("SIGNAL_REQUIRE_KALSHI=true but Kalshi WebSocket auth is not configured")
            else:
                LOGGER.warning("Kalshi WebSocket auth is not configured; using REST polling fallback")
                listeners.append(KalshiRestPollingListener(self.config, self.engine, self.taxonomy))
        if "azuro" in enabled:
            listeners.append(AzuroListener(self.config, self.engine, self.taxonomy))
        if "oddspapi" in enabled:
            if oddspapi_api_key():
                if env_str("ODDSPAPI_LIVE_MODE", "poll").lower() in {"ws", "websocket", "stream"}:
                    listeners.append(OddsPapiListener(self.config, self.engine))
                else:
                    listeners.append(OddsPapiRestPollingListener(self.config, self.engine))
            elif bool_env("SIGNAL_REQUIRE_ODDSPAPI", False):
                raise RuntimeError("SIGNAL_REQUIRE_ODDSPAPI=true but ODDSPAPI_KEY is not available")
            else:
                LOGGER.warning("Skipping OddsPapi live listener: ODDSPAPI_KEY is not available")
        if not listeners:
            raise RuntimeError("No signaler platforms enabled")
        if len({listener.name for listener in listeners}) < self.config.min_platforms:
            live_names = ", ".join(sorted({listener.name for listener in listeners})) or "none"
            raise RuntimeError(
                f"Only {len(listeners)} live platform(s) available ({live_names}), but "
                f"SIGNAL_MIN_PLATFORMS={self.config.min_platforms}. Cross-platform arbitrage cannot be "
                "signalled until another live source is available. Lower SIGNAL_MIN_PLATFORMS only for diagnostics."
            )
        LOGGER.info(
            "Starting live signaler with %s listeners and %s canonical clusters",
            len(listeners),
            len(self.taxonomy.clusters),
        )
        await self.send_startup_status(listeners)
        await asyncio.gather(
            self.engine.run_digest_loop(),
            *(listener.run_forever() for listener in listeners),
        )

    async def send_startup_status(self, listeners: Sequence[ReconnectingListener]) -> None:
        names = ", ".join(sorted({listener.name for listener in listeners}))
        cross_platform_clusters = self.taxonomy.cross_platform_cluster_count()
        message = (
            "wc-arbbot live signaler started\n"
            f"Live sources: {names}\n"
            f"Canonical clusters: {len(self.taxonomy.clusters)}\n"
            f"Cross-platform clusters: {cross_platform_clusters}\n"
            f"Min platforms per alert: {self.config.min_platforms}\n"
            f"Top alerts per digest: {self.config.max_alerts_per_digest}"
        )
        if cross_platform_clusters == 0:
            message += "\nNo cross-platform overlaps were mapped yet; waiting for OddsPapi/Pinnacle/1xBet overlap data."
        if self.config.dry_run or not self.engine.telegram:
            LOGGER.info(message.replace("\n", " | "))
            return
        try:
            await self.engine.telegram.send_message(message)
        except Exception:
            LOGGER.exception("Failed to send startup Telegram status")


def choose_covering_outcomes(
    keys: Iterable[str],
    *,
    market_type: str | None = None,
    assume_multigroup_exhaustive: bool,
    min_multi_outcomes: int,
) -> tuple[str, ...]:
    key_set = {normalize_outcome_key(key) for key in keys if key}
    if market_type == "moneyline":
        if "draw" in key_set and len(key_set) >= 3:
            return tuple(sorted(key_set))
        if len(key_set) == 2:
            return tuple(sorted(key_set))
    binary_pairs = [
        ("yes", "no"),
        ("over", "under"),
        ("home", "away"),
        ("draw", "not_draw"),
    ]
    for left, right in binary_pairs:
        if left in key_set and right in key_set:
            return (left, right)
    if assume_multigroup_exhaustive and len(key_set) >= min_multi_outcomes:
        return tuple(sorted(key_set))
    return ()


def normalize_outcome_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    replacements = {
        "y": "yes",
        "true": "yes",
        "n": "no",
        "false": "no",
        "o": "over",
        "u": "under",
    }
    text = replacements.get(text, text)
    text = text.replace(" ", "_").replace("/", "_")
    return "".join(ch for ch in text if ch.isalnum() or ch in {"_", "-"}).strip("_") or "unknown"


def moneyline_selection_from_market(market_name: str) -> str:
    text = market_name.lower()
    if "draw" in text:
        return "draw"
    match = re.search(r"\bwill\s+(.+?)\s+win\b", text)
    if not match:
        match = re.search(r"^(.+?)\s+(?:to\s+)?win\b", text)
    if not match:
        return ""
    selection = re.sub(r"\bon\s+\d{4}-\d{2}-\d{2}\b", "", match.group(1)).strip()
    return normalize_outcome_key(selection)


def parse_price_size_levels(raw: Any) -> list[tuple[float, float]]:
    levels: list[tuple[float, float]] = []
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return levels
    for level in raw:
        price = None
        size = None
        if isinstance(level, Mapping):
            price = level.get("price")
            size = level.get("size") or level.get("quantity") or level.get("count")
        elif isinstance(level, Sequence) and not isinstance(level, (str, bytes)) and len(level) >= 2:
            price = level[0]
            size = level[1]
        p = float_or_none(price)
        s = float_or_none(size)
        if p is not None and s is not None:
            levels.append((p, s))
    return levels


def decode_ws_payload(raw: Any) -> list[Mapping[str, Any]]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if not isinstance(raw, str):
        return []
    raw = raw.strip()
    if not raw or raw in {"PONG", "pong"}:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        LOGGER.debug("Ignoring non-JSON websocket message: %s", raw[:200])
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    return [payload] if isinstance(payload, Mapping) else []


def extract_odds_like_updates(payload: Any) -> list[Mapping[str, Any]]:
    updates: list[Mapping[str, Any]] = []
    stack = [payload]
    while stack:
        item = stack.pop()
        if isinstance(item, Mapping):
            if any(key in item for key in ("currentOdds", "decimalOdds", "odds")) and any(
                key in item for key in ("outcomeId", "outcome_id", "id")
            ):
                updates.append(item)
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return updates


def kalshi_auth_headers(path: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    bearer = os.environ.get("KALSHI_BEARER_TOKEN")
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    api_key = os.environ.get("KALSHI_API_KEY") or os.environ.get("KALSHI_ACCESS_KEY")
    if api_key:
        headers["KALSHI-ACCESS-KEY"] = api_key
    private_key = os.environ.get("KALSHI_PRIVATE_KEY")
    if api_key and private_key:
        signature = kalshi_signature(private_key, path)
        headers.update(signature)
    return headers


def has_kalshi_auth() -> bool:
    return bool(
        os.environ.get("KALSHI_BEARER_TOKEN")
        or os.environ.get("KALSHI_API_KEY")
        or os.environ.get("KALSHI_ACCESS_KEY")
    )


def kalshi_signature(private_key_pem: str, path: str) -> dict[str, str]:
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
    except ImportError as exc:
        raise RuntimeError("cryptography is required for KALSHI_PRIVATE_KEY auth") from exc

    timestamp = str(int(time.time() * 1000))
    message = f"{timestamp}GET{path}".encode("utf-8")
    key = serialization.load_pem_private_key(private_key_pem.encode("utf-8"), password=None)
    signed = key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signed).decode("ascii"),
    }


def platform_fee_bps(platform: str) -> float:
    primary, alias = platform_env_keys(platform, "FEE_BPS")
    return float_env(primary, float_env(alias, 0.0))


def platform_fixed_fee(platform: str) -> float:
    primary, alias = platform_env_keys(platform, "FIXED_FEE_USD")
    return float_env(primary, float_env(alias, 0.0))


def platform_env_keys(platform: str, suffix: str) -> tuple[str, str]:
    raw = platform.upper().replace("-", "_")
    normalized = raw.replace("1XBET", "ONEXBET")
    return f"{normalized}_{suffix}", f"{raw}_{suffix}"


def chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(items), max(1, size)):
        yield items[index : index + size]


def as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def first_non_empty(values: Iterable[str]) -> str | None:
    for value in values:
        if value:
            return value
    return None


def first_non_null(values: Iterable[Any]) -> Any | None:
    for value in values:
        if value is not None:
            return value
    return None


def stable_leg_id(item: Any) -> str:
    leg = item[0] if isinstance(item, tuple) and item else item
    ref = getattr(leg, "ref", None)
    if ref is not None:
        return f"{ref.platform}:{ref.market_id}:{ref.instrument_id}:{ref.outcome_key}"
    return ":".join(
        str(part)
        for part in (
            getattr(leg, "platform", ""),
            getattr(leg, "instrument_id", ""),
            getattr(leg, "outcome_key", ""),
        )
        if part
    )


def opportunity_latest_update(opportunity: ArbitrageOpportunity) -> float:
    return max((leg.updated_at for leg in opportunity.legs), default=0.0)


def float_or_none(value: Any) -> float | None:
    return safe_float(value)


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


def bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def env_csv(name: str) -> list[str]:
    value = os.environ.get(name, "")
    return [part.strip() for part in value.split(",") if part.strip()]


async def cli_async(args: argparse.Namespace) -> int:
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    config = SignalerConfig.from_env(args.taxonomy)
    if args.dry_run:
        config = dataclasses.replace(config, dry_run=True)
    if args.min_profit_pct is not None:
        config = dataclasses.replace(config, min_profit_pct=args.min_profit_pct)
    if args.target_payout is not None:
        config = dataclasses.replace(config, target_payout_usd=args.target_payout)
    await LiveSignaler(config).run()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run wc-arbbot live arbitrage signaler.")
    parser.add_argument("--taxonomy", help="Path to taxonomy JSON generated by nlp_mapper.py.")
    parser.add_argument("--dry-run", action="store_true", help="Log alerts without Telegram sends.")
    parser.add_argument("--target-payout", type=float, help="Equalized payout target per outcome.")
    parser.add_argument("--min-profit-pct", type=float, help="Minimum net edge percentage.")
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
