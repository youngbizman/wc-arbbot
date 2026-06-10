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
import json
import logging
import os
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
    parse_mode: str = "MarkdownV2"
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
        url = f"{self.config.api_base_url.rstrip('/')}/bot{self.config.bot_token}/sendMessage"
        payload = {
            "chat_id": self.config.chat_id,
            "text": text,
            "parse_mode": self.config.parse_mode,
            "disable_web_page_preview": self.config.disable_web_page_preview,
        }
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
    profit_pct = float_or_none(data.get("profit_pct") or data.get("profitPct")) or 0.0
    total_cost = float_or_none(data.get("total_cost") or data.get("totalCost")) or 0.0
    profit = float_or_none(data.get("profit")) or 0.0
    payout = float_or_none(data.get("target_payout") or data.get("targetPayout")) or 0.0
    cluster_id = str(data.get("canonical_id") or data.get("canonicalId") or "")

    lines = [
        f"*{md_escape('+EV Arbitrage Signal')}*",
        "",
        f"*Event:* {md_escape(event_name)}",
        f"*Market:* {md_escape(market_name)}",
        f"*Edge:* `{md_code(format_pct(profit_pct))}`",
        f"*Target payout:* `{md_code(format_money(payout))}`",
        f"*Total stake:* `{md_code(format_money(total_cost))}`",
        f"*Guaranteed profit:* `{md_code(format_money(profit))}`",
    ]
    if cluster_id:
        lines.append(f"*Cluster:* `{md_code(cluster_id)}`")
    lines.extend(["", "*Required legs:*"])

    for index, leg in enumerate(legs, start=1):
        platform = str(leg.get("platform") or "unknown").title()
        outcome = str(leg.get("outcome_key") or leg.get("outcome") or "outcome")
        url = str(leg.get("url") or "")
        stake = float_or_none(leg.get("stake") or leg.get("cost")) or 0.0
        vwap = float_or_none(leg.get("vwap") or leg.get("effective_probability"))
        odds = float_or_none(leg.get("effective_odds"))
        instrument = str(leg.get("instrument_id") or leg.get("instrumentId") or "")
        market = str(leg.get("market_name") or leg.get("marketName") or "")

        platform_text = md_link(platform, url) if url else md_escape(platform)
        lines.append("")
        lines.append(f"{index}\\. *{platform_text}* - `{md_code(outcome)}`")
        if market:
            lines.append(f"   *Book market:* {md_escape(market)}")
        if instrument:
            lines.append(f"   *Instrument:* `{md_code(short_id(instrument))}`")
        if vwap is not None:
            lines.append(f"   *VWAP price:* `{md_code(format_price(vwap))}`")
        if odds is not None:
            lines.append(f"   *Effective odds:* `{md_code(format_price(odds))}x`")
        lines.append(f"   *Stake:* `{md_code(format_money(stake))}`")

    stale = data.get("max_data_age_seconds") or data.get("maxDataAgeSeconds")
    if stale is not None:
        lines.extend(["", f"_Max quote age: {md_escape(str(round(float(stale), 3)))}s_"])
    lines.append("")
    lines.append(md_escape("Manual execution only. Re-check markets before placing bets."))
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
        await bot.send_message(md_escape(args.message))
    elif args.alert_json:
        with open(args.alert_json, "r", encoding="utf-8") as handle:
            await bot.send_alert(json.load(handle))
    else:
        await bot.send_message(md_escape("wc-arbbot Telegram test alert"))
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
