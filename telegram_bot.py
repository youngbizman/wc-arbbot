"""Telegram alert transport for wc-arbbot.

Credentials are read from environment variables:

    TELEGRAM_BOT_KEY      Bot token from BotFather
    TELEGRAM_GROUP_ID     Target group/chat ID

Aliases TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are also supported.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import datetime as dt
import html
import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


LOGGER = logging.getLogger("wc_arbbot.telegram")


MDV2_SPECIALS = r"_*[]()~`>#+-=|{}.!"


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str
    chat_id: str
    api_base_url: str = "https://api.telegram.org"
    timeout_seconds: float = 15.0
    parse_mode: str = "HTML"
    disable_web_page_preview: bool = True

    @classmethod
    def from_env(cls) -> "TelegramConfig":
        token = os.environ.get("TELEGRAM_BOT_KEY") or os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_GROUP_ID") or os.environ.get("TELEGRAM_CHAT_ID")
        if not token:
            raise RuntimeError("TELEGRAM_BOT_KEY or TELEGRAM_BOT_TOKEN is required")
        if not chat_id:
            raise RuntimeError("TELEGRAM_GROUP_ID or TELEGRAM_CHAT_ID is required")
        return cls(
            bot_token=token,
            chat_id=chat_id,
            api_base_url=os.environ.get("TELEGRAM_API_BASE_URL", cls.api_base_url),
            timeout_seconds=float(os.environ.get("TELEGRAM_TIMEOUT_SECONDS", cls.timeout_seconds)),
            parse_mode=os.environ.get("TELEGRAM_PARSE_MODE", cls.parse_mode),
            disable_web_page_preview=env_bool(
                "TELEGRAM_DISABLE_WEB_PREVIEW",
                cls.disable_web_page_preview,
            ),
        )


class TelegramBot:
    def __init__(self, config: TelegramConfig | None = None) -> None:
        self.config = config or TelegramConfig.from_env()

    async def send_alert(self, opportunity: Any) -> Mapping[str, Any]:
        return await self.send_message(format_arbitrage_alert(opportunity))

    async def send_message(self, text: str) -> Mapping[str, Any]:
        return await asyncio.to_thread(self._send_message_sync, text)

    def _send_message_sync(self, text: str) -> Mapping[str, Any]:
        return self._send_payload_sync(text, self.config.parse_mode)

    def _send_payload_sync(self, text: str, parse_mode: str | None) -> Mapping[str, Any]:
        url = f"{self.config.api_base_url.rstrip('/')}/bot{self.config.bot_token}/sendMessage"
        payload = {
            "chat_id": self.config.chat_id,
            "text": text,
            "disable_web_page_preview": self.config.disable_web_page_preview,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        body = urllib.parse.urlencode(payload).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.config.timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            if parse_mode and exc.code == 400 and "can't parse entities" in error_body:
                LOGGER.warning("Telegram rejected formatted message; retrying as plain text")
                return self._send_payload_sync(strip_markup(text), None)
            raise RuntimeError(f"Telegram HTTP {exc.code}: {error_body}") from exc
        result = json.loads(raw)
        if not result.get("ok"):
            raise RuntimeError(f"Telegram send failed: {result}")
        LOGGER.info("Sent Telegram alert to %s", self.config.chat_id)
        return result


def format_arbitrage_alert(opportunity: Any) -> str:
    data = to_mapping(opportunity)
    legs = [to_mapping(leg) for leg in data.get("legs", [])]

    event_name = str(data.get("event_name") or data.get("eventName") or "Unknown event")
    market_name = str(data.get("market_name") or data.get("marketName") or "Overlapping market")
    event_time = data.get("event_time") or data.get("eventTime")
    market_type = str(data.get("market_type") or data.get("marketType") or "")
    line = float_or_none(data.get("line"))
    profit_pct = float_or_none(data.get("profit_pct") or data.get("profitPct")) or 0.0
    total_cost = float_or_none(data.get("total_cost") or data.get("totalCost")) or 0.0
    profit = float_or_none(data.get("profit")) or 0.0
    payout = float_or_none(data.get("target_payout") or data.get("targetPayout")) or 0.0
    market_label = format_market_label(market_type, market_name, line)

    lines = [
        "<b>+EV Arbitrage Signal</b>",
        "",
        f"<b>Game:</b> {html_escape(event_name)}",
        f"<b>Date:</b> {html_escape(format_event_time(event_time))}",
        f"<b>Market:</b> {html_escape(market_label)}",
        "",
        f"<b>ROI:</b> <code>{html_escape(format_pct(profit_pct))}</code>",
        f"<b>Total investment:</b> <code>{html_escape(format_money(total_cost))}</code>",
        f"<b>Total outcome:</b> <code>{html_escape(format_money(payout))}</code>",
        f"<b>Net profit:</b> <code>{html_escape(format_money(profit))}</code>",
    ]
    lines.extend(["", "<b>Manual legs:</b>"])

    for index, leg in enumerate(legs, start=1):
        platform = platform_label(str(leg.get("platform") or "unknown"))
        outcome = str(leg.get("outcome_key") or leg.get("outcome") or "outcome")
        url = str(leg.get("url") or "")
        stake = float_or_none(leg.get("stake") or leg.get("cost")) or 0.0
        vwap = float_or_none(leg.get("vwap") or leg.get("effective_probability"))
        odds = float_or_none(leg.get("effective_odds"))
        market = str(leg.get("market_name") or leg.get("marketName") or "")

        platform_text = html_link(platform, url) if url else html_escape(platform)
        lines.append("")
        lines.append(
            f"{index}. On <b>{platform_text}</b> put "
            f"<code>{html_escape(format_money(stake))}</code> on "
            f"<b>{html_escape(human_outcome(outcome, market))}</b>"
        )
        if market:
            lines.append(f"   <b>Book market:</b> {html_escape(market)}")
        if vwap is not None:
            lines.append(f"   <b>VWAP price:</b> <code>{html_escape(format_price(vwap))}</code>")
        if odds is not None:
            lines.append(f"   <b>Effective odds:</b> <code>{html_escape(format_price(odds))}x</code>")

    stale = data.get("max_data_age_seconds") or data.get("maxDataAgeSeconds")
    if stale is not None:
        lines.extend(["", f"<i>Max quote age: {html_escape(str(round(float(stale), 3)))}s</i>"])
    lines.append("")
    lines.append(html_escape("Manual execution only. Re-check markets before placing bets."))
    return "\n".join(lines)


def to_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if hasattr(value, "asdict"):
        return value.asdict()
    raise TypeError(f"Cannot format alert payload of type {type(value)!r}")


def md_escape(value: str) -> str:
    return "".join(f"\\{char}" if char in MDV2_SPECIALS else char for char in value)


def md_code(value: str) -> str:
    return value.replace("\\", "\\\\").replace("`", "\\`")


def md_link(label: str, url: str) -> str:
    safe_label = md_escape(label)
    safe_url = url.replace("\\", "\\\\").replace(")", "\\)")
    return f"[{safe_label}]({safe_url})"


def html_escape(value: str) -> str:
    return html.escape(value, quote=True)


def html_link(label: str, url: str) -> str:
    return f'<a href="{html_escape(url)}">{html_escape(label)}</a>'


def strip_markup(value: str) -> str:
    value = re.sub(r"<a\s+href=\"([^\"]+)\">([^<]+)</a>", r"\2 (\1)", value)
    value = re.sub(r"</?(?:b|i|code)>", "", value)
    return html.unescape(value)


def platform_label(value: str) -> str:
    labels = {
        "1xbet": "1xBet",
        "azuro": "Azuro",
        "dexsport": "Dexsport",
        "kalshi": "Kalshi",
        "oddspapi": "OddsPapi",
        "pinnacle": "Pinnacle",
        "polymarket": "Polymarket",
    }
    return labels.get(value.strip().lower(), value.strip().title() or "Unknown")


def format_market_label(market_type: str, market_name: str, line: float | None) -> str:
    labels = {
        "moneyline": "Moneyline",
        "draw_no_bet": "Draw no bet",
        "exact_score": "Exact score",
        "both_teams_to_score": "Both teams to score",
        "total_goals": "Game total",
        "team_total_goals": "Team total",
        "handicap": "Handicap",
        "halftime_result": "Halftime result",
        "first_half": "First half",
        "player_goal": "Player prop",
        "first_goalscorer": "First goalscorer",
        "golden_boot": "Golden boot",
        "outright_winner": "Outright winner",
        "group_winner": "Group winner",
        "stage_of_elimination": "Stage of elimination",
    }
    clean_type = labels.get(market_type.strip().lower(), market_type.replace("_", " ").title())
    if line is not None and clean_type:
        return f"{clean_type} {line:g}"
    if clean_type:
        return clean_type
    return market_name or "Overlapping market"


def format_event_time(value: Any) -> str:
    if not value:
        return "Unknown"
    raw = str(value)
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def human_outcome(outcome: str, market_name: str) -> str:
    clean = outcome.replace("_", " ").strip()
    if clean.lower() in {"yes", "no", "over", "under", "home", "away", "draw"}:
        return clean.upper()
    if clean:
        return clean.title()
    return market_name or "selection"


def format_money(value: float) -> str:
    return f"${value:,.2f}"


def format_pct(value: float) -> str:
    return f"{value:.2f}%"


def format_price(value: float) -> str:
    if abs(value) >= 100:
        return f"{value:,.2f}"
    return f"{value:.6f}".rstrip("0").rstrip(".")


def short_id(value: str, limit: int = 18) -> str:
    if len(value) <= limit:
        return value
    return f"{value[:8]}...{value[-6:]}"


def float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


async def cli_async(args: argparse.Namespace) -> int:
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    bot = TelegramBot()
    if args.message:
        await bot.send_message(html_escape(args.message))
    elif args.alert_json:
        with open(args.alert_json, "r", encoding="utf-8") as handle:
            await bot.send_alert(json.load(handle))
    else:
        await bot.send_message(html_escape("wc-arbbot Telegram test alert"))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send wc-arbbot Telegram alerts.")
    parser.add_argument("--message", help="Plain text test message.")
    parser.add_argument("--alert-json", help="Path to an alert payload JSON file.")
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
