"""Market taxonomy pre-compiler for the World Cup arbitrage signal engine.

The mapper is deliberately separate from live pricing. It performs heavier
normalization, Q-gram filtering, fuzzy scoring, and optional embedding scoring
once per discovery cycle, then emits a static taxonomy map. The hot arbitrage
loop can then use direct O(1) lookups from platform instrument IDs to canonical
market clusters.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import difflib
import hashlib
import json
import logging
import math
import os
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from indexer import MarketOutcome, MarketRecord, Platform, parse_datetime
except ImportError:  # pragma: no cover - allows standalone schema validation.
    MarketOutcome = Any  # type: ignore
    MarketRecord = Any  # type: ignore
    Platform = Any  # type: ignore

    def parse_datetime(value: Any) -> dt.datetime | None:
        return None


LOGGER = logging.getLogger("wc_arbbot.mapper")
UTC = dt.timezone.utc


STOP_WORDS = {
    "a",
    "an",
    "and",
    "at",
    "be",
    "by",
    "during",
    "for",
    "from",
    "have",
    "in",
    "is",
    "it",
    "match",
    "of",
    "on",
    "or",
    "the",
    "to",
    "vs",
    "will",
    "win",
    "with",
}


TEAM_ALIASES = {
    "arg": "argentina",
    "ar": "argentina",
    "aus": "australia",
    "bel": "belgium",
    "bosnia herzegovina": "bosnia and herzegovina",
    "bosnia & herzegovina": "bosnia and herzegovina",
    "bra": "brazil",
    "brasil": "brazil",
    "can": "canada",
    "cabo verde": "cape verde",
    "cap verde": "cape verde",
    "curacao": "curacao",
    "czech republic": "czechia",
    "cze": "czechia",
    "dr congo": "democratic republic of the congo",
    "drc": "democratic republic of the congo",
    "eng": "england",
    "fra": "france",
    "fr": "france",
    "ger": "germany",
    "deutschland": "germany",
    "gha": "ghana",
    "iran": "iran",
    "ir iran": "iran",
    "ita": "italy",
    "jpn": "japan",
    "kor": "korea republic",
    "south korea": "korea republic",
    "mex": "mexico",
    "nzl": "new zealand",
    "por": "portugal",
    "qat": "qatar",
    "rsa": "south africa",
    "sa": "south africa",
    "sui": "switzerland",
    "swiss": "switzerland",
    "ukraine": "ukraine",
    "usa": "united states",
    "u s a": "united states",
    "us": "united states",
    "u.s.": "united states",
    "united states of america": "united states",
}


MARKET_ALIASES = {
    "h2h": "moneyline",
    "head to head": "moneyline",
    "match winner": "moneyline",
    "money line": "moneyline",
    "1x2": "moneyline",
    "draw no bet": "draw_no_bet",
    "exact score": "exact_score",
    "correct score": "exact_score",
    "soccer exact score": "exact_score",
    "both teams to score": "both_teams_to_score",
    "btts": "both_teams_to_score",
    "totals": "total_goals",
    "total": "total_goals",
    "over under": "total_goals",
    "o u": "total_goals",
    "spread": "handicap",
    "spreads": "handicap",
    "handicap": "handicap",
    "team totals": "team_total_goals",
    "team total": "team_total_goals",
    "halftime result": "halftime_result",
    "half time result": "halftime_result",
    "first half": "first_half",
    "player goals": "player_goal",
    "to score": "player_goal",
    "anytime goalscorer": "player_goal",
    "first goalscorer": "first_goalscorer",
    "golden boot": "golden_boot",
    "top goalscorer": "golden_boot",
    "world cup winner": "outright_winner",
    "winner": "outright_winner",
    "group winner": "group_winner",
    "win group": "group_winner",
    "stage of elimination": "stage_of_elimination",
    "round of 16": "reach_round_of_16",
    "quarterfinal": "reach_quarterfinal",
    "semifinal": "reach_semifinal",
    "final": "reach_final",
}


PERIOD_ALIASES = {
    "1h": "first_half",
    "1st half": "first_half",
    "first half": "first_half",
    "half time": "first_half",
    "halftime": "first_half",
    "full time": "full_time",
    "fulltime": "full_time",
    "match": "full_time",
}


@dataclass(frozen=True)
class NormalizedText:
    original: str
    normalized: str
    tokens: tuple[str, ...]
    qgrams: frozenset[str]


@dataclass(frozen=True)
class NormalizedOutcome:
    outcome_id: str
    name: str
    normalized_name: str
    side: str | None = None
    line: float | None = None
    score: tuple[int, int] | None = None
    participant: str | None = None


@dataclass(frozen=True)
class NormalizedMarket:
    platform: str
    market_id: str
    event_id: str | None
    event_name: str
    market_name: str
    market_type: str
    period: str
    participants: tuple[str, ...]
    event_scope: str
    line: float | None
    start_time: str | None
    resolve_time: str | None
    mutually_exclusive_group_id: str | None
    outcomes: tuple[NormalizedOutcome, ...]
    source: Mapping[str, Any] = field(default_factory=dict)

    @property
    def lookup_ids(self) -> tuple[str, ...]:
        ids = [f"{self.platform}:{self.market_id}"]
        for outcome in self.outcomes:
            ids.append(f"{self.platform}:{self.market_id}:{outcome.outcome_id}")
        return tuple(ids)

    def canonical_fingerprint(self) -> str:
        fields = [
            self.event_scope,
            self.market_type,
            self.period,
            ",".join(self.participants),
            "" if self.line is None else f"{self.line:g}",
            market_selection_key(self),
            self.start_time[:10] if self.start_time else "",
            self.resolve_time[:10] if self.resolve_time else "",
        ]
        return "|".join(fields)


@dataclass(frozen=True)
class MatchCandidate:
    left_id: str
    right_id: str
    left_platform: str
    right_platform: str
    score: float
    reasons: Mapping[str, float]


@dataclass(frozen=True)
class MarketCluster:
    canonical_id: str
    markets: tuple[str, ...]
    platforms: tuple[str, ...]
    event_scope: str
    market_type: str
    period: str
    participants: tuple[str, ...]
    line: float | None
    candidates: tuple[MatchCandidate, ...]


@dataclass(frozen=True)
class TaxonomyMap:
    generated_at: str
    markets: Mapping[str, NormalizedMarket]
    clusters: tuple[MarketCluster, ...]
    lookup: Mapping[str, str]
    rejected_pairs: tuple[MatchCandidate, ...] = ()

    def asdict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "markets": {
                market_id: dataclasses.asdict(market)
                for market_id, market in self.markets.items()
            },
            "clusters": [dataclasses.asdict(cluster) for cluster in self.clusters],
            "lookup": dict(self.lookup),
            "rejected_pairs": [dataclasses.asdict(pair) for pair in self.rejected_pairs],
        }


@dataclass(frozen=True)
class MapperConfig:
    qgram_size: int = 3
    min_qgram_jaccard: float = 0.08
    min_match_score: float = 0.72
    high_confidence_score: float = 0.84
    max_time_delta_minutes: int = 180
    enable_semantic: bool = True
    semantic_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    semantic_weight: float = 0.18
    max_rejected_pairs: int = 500

    @classmethod
    def from_env(cls) -> "MapperConfig":
        return cls(
            qgram_size=int_env("NLP_QGRAM_SIZE", cls.qgram_size),
            min_qgram_jaccard=float_env("NLP_MIN_QGRAM_JACCARD", cls.min_qgram_jaccard),
            min_match_score=float_env("NLP_MIN_MATCH_SCORE", cls.min_match_score),
            high_confidence_score=float_env("NLP_HIGH_CONFIDENCE_SCORE", cls.high_confidence_score),
            max_time_delta_minutes=int_env("NLP_MAX_TIME_DELTA_MINUTES", cls.max_time_delta_minutes),
            enable_semantic=bool_env("NLP_ENABLE_SEMANTIC", cls.enable_semantic),
            semantic_model_name=os.environ.get("NLP_EMBEDDING_MODEL", cls.semantic_model_name),
            semantic_weight=float_env("NLP_SEMANTIC_WEIGHT", cls.semantic_weight),
            max_rejected_pairs=int_env("NLP_MAX_REJECTED_PAIRS", cls.max_rejected_pairs),
        )


class SemanticScorer:
    def __init__(self, config: MapperConfig) -> None:
        self.config = config
        self.model: Any | None = None
        self.available = False
        if not config.enable_semantic:
            return
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore

            self.model = SentenceTransformer(config.semantic_model_name)
            self.available = True
        except Exception as exc:  # noqa: BLE001 - optional dependency.
            LOGGER.info("Semantic embeddings disabled: %s", exc)

    def pairwise(self, texts: Sequence[str]) -> dict[tuple[int, int], float]:
        if not self.available or not self.model or len(texts) < 2:
            return {}
        embeddings = self.model.encode(list(texts), normalize_embeddings=True)
        scores: dict[tuple[int, int], float] = {}
        for i in range(len(texts)):
            for j in range(i + 1, len(texts)):
                scores[(i, j)] = dot_product(embeddings[i], embeddings[j])
        return scores


class UnionFind:
    def __init__(self, items: Iterable[str]) -> None:
        self.parent = {item: item for item in items}

    def find(self, item: str) -> str:
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, a: str, b: str) -> None:
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a != root_b:
            self.parent[root_b] = root_a

    def groups(self) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = defaultdict(list)
        for item in self.parent:
            grouped[self.find(item)].append(item)
        return grouped


class MarketMapper:
    def __init__(self, config: MapperConfig | None = None) -> None:
        self.config = config or MapperConfig.from_env()
        self.semantic = SemanticScorer(self.config)

    def compile(self, records: Sequence[MarketRecord | Mapping[str, Any]]) -> TaxonomyMap:
        normalized = [self.normalize_market(record) for record in records]
        normalized = [market for market in normalized if market is not None]
        markets = {self.market_key(market): market for market in normalized}
        candidates, rejected = self._score_candidates(markets)

        uf = UnionFind(markets.keys())
        accepted: list[MatchCandidate] = []
        for candidate in candidates:
            if candidate.score >= self.config.min_match_score:
                uf.union(candidate.left_id, candidate.right_id)
                accepted.append(candidate)
            else:
                rejected.append(candidate)

        cluster_candidates: dict[str, list[MatchCandidate]] = defaultdict(list)
        for candidate in accepted:
            root = uf.find(candidate.left_id)
            cluster_candidates[root].append(candidate)

        clusters: list[MarketCluster] = []
        lookup: dict[str, str] = {}
        for members in uf.groups().values():
            member_markets = [markets[member] for member in members]
            canonical_id = self._cluster_id(member_markets)
            exemplar = self._choose_exemplar(member_markets)
            cluster = MarketCluster(
                canonical_id=canonical_id,
                markets=tuple(sorted(members)),
                platforms=tuple(sorted({market.platform for market in member_markets})),
                event_scope=exemplar.event_scope,
                market_type=exemplar.market_type,
                period=exemplar.period,
                participants=exemplar.participants,
                line=exemplar.line,
                candidates=tuple(
                    sorted(
                        cluster_candidates.get(uf.find(members[0]), []),
                        key=lambda c: c.score,
                        reverse=True,
                    )
                ),
            )
            clusters.append(cluster)
            for market_id in members:
                market = markets[market_id]
                lookup[market_id] = canonical_id
                for lookup_id in market.lookup_ids:
                    lookup[lookup_id] = canonical_id

        clusters.sort(key=lambda cluster: (cluster.event_scope, cluster.market_type, cluster.canonical_id))
        return TaxonomyMap(
            generated_at=dt.datetime.now(tz=UTC).isoformat(),
            markets=markets,
            clusters=tuple(clusters),
            lookup=lookup,
            rejected_pairs=tuple(sorted(rejected, key=lambda c: c.score, reverse=True)[: self.config.max_rejected_pairs]),
        )

    def normalize_market(self, record: MarketRecord | Mapping[str, Any]) -> NormalizedMarket | None:
        raw = to_mapping(record)
        platform = platform_value(raw.get("platform"))
        market_id = str(raw.get("market_id") or raw.get("marketId") or raw.get("id") or "")
        if not platform or not market_id:
            return None

        event_name = str(raw.get("event_name") or raw.get("eventName") or raw.get("parent_event_name") or "")
        market_name = str(raw.get("market_name") or raw.get("marketName") or raw.get("question") or "")
        market_type_raw = str(raw.get("market_type") or raw.get("marketType") or "")
        parent_event_name = str(raw.get("parent_event_name") or raw.get("parentEventName") or "")
        full_text = " ".join(part for part in (event_name, market_name, market_type_raw, parent_event_name) if part)
        participant_text = " ".join(part for part in (event_name, parent_event_name) if part)

        participants = extract_participants(participant_text) or extract_participants(full_text)
        event_scope = infer_event_scope(full_text, participants)
        market_type = infer_market_type(market_type_raw, market_name, parent_event_name)
        period = infer_period(full_text)
        line = extract_line(full_text)
        outcomes = tuple(normalize_outcomes(raw.get("outcomes") or [], market_name, self.config.qgram_size))
        start_time = coerce_iso(raw.get("start_time") or raw.get("startTime"))
        resolve_time = coerce_iso(raw.get("resolve_time") or raw.get("resolveTime") or raw.get("close_time"))

        if market_type == "exact_score":
            score = extract_score(full_text)
            if score is not None:
                outcomes = tuple(
                    dataclasses.replace(outcome, score=outcome.score or score)
                    for outcome in outcomes
                )

        return NormalizedMarket(
            platform=platform,
            market_id=market_id,
            event_id=none_if_empty(raw.get("event_id") or raw.get("eventId")),
            event_name=event_name,
            market_name=market_name,
            market_type=market_type,
            period=period,
            participants=participants,
            event_scope=event_scope,
            line=line,
            start_time=start_time,
            resolve_time=resolve_time,
            mutually_exclusive_group_id=none_if_empty(
                raw.get("mutually_exclusive_group_id") or raw.get("mutuallyExclusiveGroupId")
            ),
            outcomes=outcomes,
            source=raw,
        )

    def market_key(self, market: NormalizedMarket) -> str:
        return f"{market.platform}:{market.market_id}"

    def _score_candidates(
        self,
        markets: Mapping[str, NormalizedMarket],
    ) -> tuple[list[MatchCandidate], list[MatchCandidate]]:
        ids = list(markets.keys())
        texts = [
            f"{markets[mid].event_name} {markets[mid].market_name} {markets[mid].market_type}"
            for mid in ids
        ]
        semantic_scores = self.semantic.pairwise(texts)
        by_block: dict[str, list[str]] = defaultdict(list)
        for market_id, market in markets.items():
            for block in candidate_blocks(market):
                by_block[block].append(market_id)

        seen: set[tuple[str, str]] = set()
        candidates: list[MatchCandidate] = []
        rejected: list[MatchCandidate] = []
        id_index = {market_id: idx for idx, market_id in enumerate(ids)}

        for block_ids in by_block.values():
            if len(block_ids) < 2:
                continue
            for i, left_id in enumerate(block_ids):
                left = markets[left_id]
                for right_id in block_ids[i + 1 :]:
                    right = markets[right_id]
                    if left.platform == right.platform:
                        continue
                    pair = tuple(sorted((left_id, right_id)))
                    if pair in seen:
                        continue
                    seen.add(pair)
                    semantic_key = tuple(sorted((id_index[left_id], id_index[right_id])))
                    candidate = self.score_pair(
                        left_id,
                        left,
                        right_id,
                        right,
                        semantic_scores.get(semantic_key),
                    )
                    if candidate.score >= self.config.min_match_score:
                        candidates.append(candidate)
                    else:
                        rejected.append(candidate)
        candidates.sort(key=lambda candidate: candidate.score, reverse=True)
        return candidates, rejected

    def score_pair(
        self,
        left_id: str,
        left: NormalizedMarket,
        right_id: str,
        right: NormalizedMarket,
        semantic_score: float | None,
    ) -> MatchCandidate:
        left_text = normalize_text(
            f"{left.event_name} {left.market_name} {left.market_type}",
            q=self.config.qgram_size,
        )
        right_text = normalize_text(
            f"{right.event_name} {right.market_name} {right.market_type}",
            q=self.config.qgram_size,
        )
        qgram_score = jaccard(left_text.qgrams, right_text.qgrams)
        if qgram_score < self.config.min_qgram_jaccard and not hard_entity_overlap(left, right):
            return MatchCandidate(
                left_id=left_id,
                right_id=right_id,
                left_platform=left.platform,
                right_platform=right.platform,
                score=0.0,
                reasons={"qgram": qgram_score},
            )

        token_score = jaccard(set(left_text.tokens), set(right_text.tokens))
        sequence_score = difflib.SequenceMatcher(None, left_text.normalized, right_text.normalized).ratio()
        participant_score = participant_similarity(left.participants, right.participants)
        market_type_score = 1.0 if left.market_type == right.market_type else alias_similarity(left.market_type, right.market_type)
        period_score = 1.0 if left.period == right.period else 0.0
        line_score = line_similarity(left.line, right.line)
        time_score = time_similarity(left.start_time, right.start_time, self.config.max_time_delta_minutes)
        outcome_score = outcome_similarity(left.outcomes, right.outcomes)
        selection_score = selection_similarity(left, right)
        if selection_score == 0.0:
            return MatchCandidate(
                left_id=left_id,
                right_id=right_id,
                left_platform=left.platform,
                right_platform=right.platform,
                score=0.0,
                reasons={"selection": selection_score, "qgram": qgram_score},
            )
        semantic = semantic_score if semantic_score is not None else 0.0

        score = (
            0.20 * participant_score
            + 0.16 * market_type_score
            + 0.12 * period_score
            + 0.12 * line_score
            + 0.10 * time_score
            + 0.10 * outcome_score
            + 0.03 * selection_score
            + 0.09 * token_score
            + 0.08 * sequence_score
            + self.config.semantic_weight * semantic
        )
        score = min(1.0, score / (1.0 + self.config.semantic_weight))

        reasons = {
            "participant": participant_score,
            "market_type": market_type_score,
            "period": period_score,
            "line": line_score,
            "time": time_score,
            "outcome": outcome_score,
            "selection": selection_score,
            "token": token_score,
            "sequence": sequence_score,
            "qgram": qgram_score,
            "semantic": semantic,
        }
        return MatchCandidate(
            left_id=left_id,
            right_id=right_id,
            left_platform=left.platform,
            right_platform=right.platform,
            score=round(score, 6),
            reasons={key: round(value, 6) for key, value in reasons.items()},
        )

    def _choose_exemplar(self, markets: Sequence[NormalizedMarket]) -> NormalizedMarket:
        return sorted(
            markets,
            key=lambda market: (
                -len(market.participants),
                market.market_type == "unknown",
                market.platform,
                market.market_id,
            ),
        )[0]

    def _cluster_id(self, markets: Sequence[NormalizedMarket]) -> str:
        exemplar = self._choose_exemplar(markets)
        digest = hashlib.sha1(
            "||".join(sorted(m.canonical_fingerprint() for m in markets)).encode("utf-8")
        ).hexdigest()[:12]
        slug = slugify(
            "|".join(
                [
                    exemplar.event_scope,
                    exemplar.market_type,
                    exemplar.period,
                    ",".join(exemplar.participants),
                    "" if exemplar.line is None else f"{exemplar.line:g}",
                    market_selection_key(exemplar),
                ]
            )
        )
        return f"cmkt_{slug}_{digest}"[:120]


def normalize_text(text: str, *, q: int = 3) -> NormalizedText:
    original = text or ""
    ascii_text = unicodedata.normalize("NFKD", original).encode("ascii", "ignore").decode("ascii")
    lowered = ascii_text.lower()
    for source, target in TEAM_ALIASES.items():
        lowered = re.sub(rf"\b{re.escape(source)}\b", target, lowered)
    lowered = lowered.replace("&", " and ")
    lowered = re.sub(r"[^a-z0-9.+-]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    tokens = tuple(token for token in lowered.split() if token and token not in STOP_WORDS)
    compact = " ".join(tokens)
    return NormalizedText(
        original=original,
        normalized=compact,
        tokens=tokens,
        qgrams=frozenset(qgrams(compact, q)),
    )


def qgrams(text: str, q: int) -> set[str]:
    compact = re.sub(r"\s+", "", text)
    if not compact:
        return set()
    if len(compact) <= q:
        return {compact}
    return {compact[i : i + q] for i in range(len(compact) - q + 1)}


def jaccard(left: set[str] | frozenset[str], right: set[str] | frozenset[str]) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def extract_participants(text: str) -> tuple[str, ...]:
    normalized = normalize_text(text).normalized
    found: set[str] = set()
    for alias, canonical in TEAM_ALIASES.items():
        alias_norm = normalize_text(alias).normalized
        canonical_norm = normalize_text(canonical).normalized
        if re.search(rf"\b{re.escape(alias_norm)}\b", normalized):
            found.add(canonical_norm)
        if re.search(rf"\b{re.escape(canonical_norm)}\b", normalized):
            found.add(canonical_norm)

    vs_match = re.search(r"(.+?)\s+(?:vs|v)\s+(.+?)(?:\s+[-|:]|\?|$)", text.lower())
    if vs_match:
        for side in (vs_match.group(1), vs_match.group(2)):
            side_norm = normalize_text(side).normalized
            if side_norm:
                found.add(side_norm)

    return tuple(sorted(found))


def infer_event_scope(text: str, participants: Sequence[str]) -> str:
    norm = normalize_text(text).normalized
    if len(participants) >= 2:
        return "match"
    if "group" in norm:
        return "group"
    if any(term in norm for term in ("golden boot", "golden ball", "golden glove", "fair play")):
        return "award"
    if any(term in norm for term in ("world cup winner", "win 2026 fifa world cup", "champion")):
        return "tournament"
    if "squad" in norm:
        return "squad"
    if "halftime show" in norm or "perform" in norm:
        return "entertainment"
    return "tournament"


def infer_market_type(*parts: str) -> str:
    text = normalize_text(" ".join(part for part in parts if part)).normalized
    for alias, canonical in sorted(MARKET_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        alias_norm = normalize_text(alias).normalized
        if alias_norm and re.search(rf"\b{re.escape(alias_norm)}\b", text):
            return canonical
    if re.search(r"\bo\s*/?\s*u\b|\bover\b|\bunder\b", text):
        return "total_goals"
    if re.search(r"\b[+-]\d+(\.\d+)?\b", text):
        return "handicap"
    return "unknown"


def infer_period(text: str) -> str:
    norm = normalize_text(text).normalized
    for alias, canonical in PERIOD_ALIASES.items():
        alias_norm = normalize_text(alias).normalized
        if alias_norm and re.search(rf"\b{re.escape(alias_norm)}\b", norm):
            return canonical
    return "full_time"


def extract_line(text: str) -> float | None:
    norm = text.lower()
    patterns = [
        r"(?:o/u|over/under|over|under)\s*([0-9]+(?:\.[0-9]+)?)",
        r"\(([+-]?[0-9]+(?:\.[0-9]+)?)\)",
        r"\b([+-][0-9]+(?:\.[0-9]+)?)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, norm)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
    return None


def extract_score(text: str) -> tuple[int, int] | None:
    match = re.search(r"\b(\d{1,2})\s*[-:]\s*(\d{1,2})\b", text)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def normalize_outcomes(raw_outcomes: Any, market_name: str, qgram_size: int) -> list[NormalizedOutcome]:
    values: list[Any]
    if isinstance(raw_outcomes, str):
        try:
            values = json.loads(raw_outcomes)
        except json.JSONDecodeError:
            values = []
    elif isinstance(raw_outcomes, Sequence):
        values = list(raw_outcomes)
    else:
        values = []

    outcomes: list[NormalizedOutcome] = []
    for idx, raw in enumerate(values):
        if isinstance(raw, Mapping):
            name = str(raw.get("name") or raw.get("label") or raw.get("title") or raw.get("outcome") or idx)
            outcome_id = str(raw.get("outcome_id") or raw.get("outcomeId") or raw.get("id") or idx)
        else:
            name = str(raw)
            outcome_id = str(idx)
        normalized = normalize_text(name, q=qgram_size).normalized
        side = infer_outcome_side(name)
        outcomes.append(
            NormalizedOutcome(
                outcome_id=outcome_id,
                name=name,
                normalized_name=normalized,
                side=side,
                line=extract_line(name) or extract_line(market_name),
                score=extract_score(name) or extract_score(market_name),
                participant=first_participant(name),
            )
        )
    return outcomes


def infer_outcome_side(name: str) -> str | None:
    norm = normalize_text(name).normalized
    if norm in {"yes", "y"}:
        return "yes"
    if norm in {"no", "n"}:
        return "no"
    if "over" in norm:
        return "over"
    if "under" in norm:
        return "under"
    if "draw" in norm:
        return "draw"
    return None


def first_participant(text: str) -> str | None:
    participants = extract_participants(text)
    return participants[0] if participants else None


def candidate_blocks(market: NormalizedMarket) -> set[str]:
    blocks: set[str] = set()
    date_bucket = (market.start_time or market.resolve_time or "")[:10]
    participant_key = ",".join(market.participants)
    line_key = "" if market.line is None else f"{market.line:g}"
    selection_key = market_selection_key(market)
    blocks.add(
        f"{date_bucket}|{market.event_scope}|{participant_key}|"
        f"{market.market_type}|{market.period}|{line_key}|{selection_key}"
    )
    blocks.add(f"{date_bucket}|{participant_key}|{market.market_type}|{market.period}|{selection_key}")
    blocks.add(f"{market.event_scope}|{participant_key}|{market.market_type}|{selection_key}")
    if market.mutually_exclusive_group_id:
        blocks.add(f"group:{market.platform}:{market.mutually_exclusive_group_id}")
    if participant_key:
        blocks.add(f"participants:{participant_key}:{market.market_type}")
    if selection_key:
        blocks.add(f"selection:{participant_key}:{market.market_type}:{selection_key}")
    return {block for block in blocks if block.replace("|", "").strip()}


def market_selection_key(market: NormalizedMarket) -> str:
    scores = sorted({outcome.score for outcome in market.outcomes if outcome.score is not None})
    if market.market_type == "exact_score" and scores:
        return "score:" + ",".join(f"{home}-{away}" for home, away in scores)

    subject = selection_subject(market)
    line_key = "" if market.line is None else f":line:{market.line:g}"
    if market.market_type in {"team_total_goals", "handicap"} and subject:
        return f"{market.market_type}:{subject}{line_key}"
    if market.market_type in {"player_goal", "first_goalscorer"} and subject:
        threshold = player_threshold(market.market_name)
        threshold_key = f":threshold:{threshold}" if threshold else ""
        return f"{market.market_type}:{subject}{line_key}{threshold_key}"
    return ""


def selection_similarity(left: NormalizedMarket, right: NormalizedMarket) -> float:
    left_key = market_selection_key(left)
    right_key = market_selection_key(right)
    if not left_key and not right_key:
        return 1.0
    if not left_key or not right_key:
        return 0.6
    return 1.0 if left_key == right_key else 0.0


def selection_subject(market: NormalizedMarket) -> str:
    for outcome in market.outcomes:
        if outcome.participant:
            return outcome.participant
    participant = first_participant(market.market_name)
    if participant:
        return participant
    head = re.split(r"[:|?]", market.market_name, maxsplit=1)[0]
    head = re.split(r"\bto score\b|\bover\b|\bunder\b|\bwill\b", head, maxsplit=1, flags=re.IGNORECASE)[0]
    norm = normalize_text(head).normalized
    stopwords = {
        "anytime",
        "first",
        "goal",
        "goals",
        "goalscorer",
        "player",
        "score",
        "team",
        "total",
        "will",
        "win",
    }
    tokens = [token for token in norm.split() if token not in stopwords and not token.isdigit()]
    return "-".join(tokens[:6])


def player_threshold(text: str) -> str:
    match = re.search(r"\b(\d+)\s*\+\s*(?:goal|goals|shots|assists)?\b", text, flags=re.IGNORECASE)
    return match.group(1) if match else ""


def hard_entity_overlap(left: NormalizedMarket, right: NormalizedMarket) -> bool:
    if left.participants and right.participants:
        return bool(set(left.participants) & set(right.participants))
    left_outcomes = {outcome.normalized_name for outcome in left.outcomes if outcome.normalized_name}
    right_outcomes = {outcome.normalized_name for outcome in right.outcomes if outcome.normalized_name}
    return bool(left_outcomes & right_outcomes)


def participant_similarity(left: Sequence[str], right: Sequence[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set and not right_set:
        return 0.6
    if not left_set or not right_set:
        return 0.2
    if left_set == right_set:
        return 1.0
    if left_set & right_set:
        return 0.65
    return 0.0


def alias_similarity(left: str, right: str) -> float:
    if left == "unknown" or right == "unknown":
        return 0.35
    if left in right or right in left:
        return 0.7
    return difflib.SequenceMatcher(None, left, right).ratio() * 0.55


def line_similarity(left: float | None, right: float | None) -> float:
    if left is None and right is None:
        return 0.7
    if left is None or right is None:
        return 0.25
    if abs(left - right) < 1e-9:
        return 1.0
    if abs(left - right) <= 0.25:
        return 0.55
    return 0.0


def time_similarity(left_iso: str | None, right_iso: str | None, max_delta_minutes: int) -> float:
    if not left_iso and not right_iso:
        return 0.6
    if not left_iso or not right_iso:
        return 0.2
    left = parse_datetime(left_iso)
    right = parse_datetime(right_iso)
    if not left or not right:
        return 0.2
    minutes = abs((left - right).total_seconds()) / 60.0
    if minutes <= 1:
        return 1.0
    if minutes > max_delta_minutes:
        return 0.0
    return max(0.0, 1.0 - (minutes / max_delta_minutes))


def outcome_similarity(left: Sequence[NormalizedOutcome], right: Sequence[NormalizedOutcome]) -> float:
    if not left and not right:
        return 0.5
    if not left or not right:
        return 0.2
    left_names = {outcome.normalized_name for outcome in left if outcome.normalized_name}
    right_names = {outcome.normalized_name for outcome in right if outcome.normalized_name}
    name_score = jaccard(left_names, right_names)
    left_sides = {outcome.side for outcome in left if outcome.side}
    right_sides = {outcome.side for outcome in right if outcome.side}
    side_score = jaccard(left_sides, right_sides)
    left_scores = {outcome.score for outcome in left if outcome.score is not None}
    right_scores = {outcome.score for outcome in right if outcome.score is not None}
    score_score = 1.0 if left_scores and left_scores == right_scores else 0.0
    return max(name_score, side_score, score_score)


def dot_product(left: Any, right: Any) -> float:
    try:
        return float(sum(float(a) * float(b) for a, b in zip(left, right, strict=False)))
    except TypeError:
        return 0.0


def to_mapping(record: MarketRecord | Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(record, Mapping):
        return record
    if hasattr(record, "asdict"):
        return record.asdict()
    return dataclasses.asdict(record)


def platform_value(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def none_if_empty(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def coerce_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        parsed = parse_datetime(value)
    if not parsed:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def slugify(value: str) -> str:
    value = normalize_text(value).normalized
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "unknown"


def load_markets(path: Path) -> list[Mapping[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        markets = payload.get("markets")
        if isinstance(markets, Mapping):
            return [item for item in markets.values() if isinstance(item, Mapping)]
        if isinstance(markets, list):
            return [item for item in markets if isinstance(item, Mapping)]
        for key in ("data", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, Mapping)]
    return []


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


def cli(args: argparse.Namespace) -> int:
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    records = load_markets(Path(args.input))
    mapper = MarketMapper(MapperConfig.from_env())
    taxonomy = mapper.compile(records)
    output = json.dumps(taxonomy.asdict(), indent=2)
    if args.output and args.output != "-":
        Path(args.output).write_text(output, encoding="utf-8")
    else:
        print(output)
    LOGGER.info(
        "Compiled %s markets into %s clusters",
        len(taxonomy.markets),
        len(taxonomy.clusters),
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compile market taxonomy map for wc-arbbot.")
    parser.add_argument(
        "--input",
        "-i",
        default=os.environ.get("WC_ARBBOT_MARKETS_PATH", "markets.json"),
        help="Normalized markets JSON from indexer.py.",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=os.environ.get("WC_ARBBOT_TAXONOMY_PATH", "taxonomy.json"),
        help="Write taxonomy JSON to this path. Use '-' for stdout.",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
    )
    return parser


def main() -> int:
    return cli(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
