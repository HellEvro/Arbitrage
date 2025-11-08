from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from typing import Any

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO

from arbitrage_bot.config import Settings
from arbitrage_bot.services.arbitrage_engine import ArbitrageEngine
from arbitrage_bot.services.market_discovery import MarketDiscoveryService
from arbitrage_bot.services.quote_store import QuoteStore
from arbitrage_bot.services.telegram_notifier import TelegramNotifier

log = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    arbitrage_engine: ArbitrageEngine | None = None,
    discovery: MarketDiscoveryService | None = None,
    quote_store: QuoteStore | None = None,
    notifier: TelegramNotifier | None = None,
) -> tuple[Flask, SocketIO]:
    app = Flask(__name__, static_folder="static", template_folder="templates")
    cors = settings.web.cors_origins if settings else ["*"]
    # For Socket.IO, explicitly allow localhost origins
    # Flask-SocketIO: "*" = allow all, list = specific origins
    if cors == ["*"] or (isinstance(cors, list) and "*" in cors):
        socketio_cors = "*"  # Allow all origins for development
    else:
        socketio_cors = list(cors) if isinstance(cors, list) else [cors]
        # Always add localhost variants
        localhost_origins = ["http://localhost:5152", "http://127.0.0.1:5152"]
        for origin in localhost_origins:
            if origin not in socketio_cors:
                socketio_cors.append(origin)
    # Use threading mode - Flask-SocketIO will handle async operations
    socketio = SocketIO(
        app,
        async_mode="threading",
        cors_allowed_origins=socketio_cors,
        allow_unsafe_werkzeug=True,
        logger=False,
        engineio_logger=False
    )
    log.info("Flask app created with SocketIO")

    @app.route("/api/status")
    def status() -> Any:
        log.debug("Status endpoint called")
        return jsonify({"status": "ok"})

    @app.route("/api/ranking")
    def ranking() -> Any:
        if not arbitrage_engine:
            return jsonify([])
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        opportunities = loop.run_until_complete(arbitrage_engine.get_latest())
        log.debug("API /ranking: got %d opportunities from engine", len(opportunities))
        payload = [
            {
                "symbol": opp.symbol,
                "buy_exchange": opp.buy_exchange,
                "buy_price": opp.buy_price,
                "buy_symbol": opp.buy_symbol,
                "buy_fee_pct": opp.buy_fee_pct,
                "sell_exchange": opp.sell_exchange,
                "sell_price": opp.sell_price,
                "sell_symbol": opp.sell_symbol,
                "sell_fee_pct": opp.sell_fee_pct,
                "spread_usdt": opp.spread_usdt,
                "spread_pct": opp.spread_pct,
                "gross_profit_usdt": opp.gross_profit_usdt,
                "total_fees_usdt": opp.total_fees_usdt,
                "timestamp_ms": opp.timestamp_ms,
            }
            for opp in opportunities
        ]
        log.debug("API /ranking: returning %d opportunities", len(payload))
        return jsonify(payload)

    @app.route("/")
    def index() -> Any:
        return render_template("index.html")

    @app.route("/internal/markets")
    def internal_markets() -> Any:
        if not discovery:
            return jsonify([])
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        markets = loop.run_until_complete(discovery.get_cached())
        payload = [
            {
                "symbol": market.symbol,
                "exchanges": list(market.exchanges),
                "exchange_symbols": market.exchange_symbols,
            }
            for market in markets
        ]
        return jsonify(payload)

    @app.route("/internal/quote", methods=["POST"])
    def internal_quote() -> Any:
        if not quote_store:
            return jsonify({"status": "disabled"}), 503
        data = request.get_json(silent=True) or {}
        symbol = data.get("symbol")
        exchange = data.get("exchange")
        price = data.get("price")
        timestamp_ms = data.get("timestamp_ms")
        native_symbol = data.get("exchange_symbol")
        if not symbol or not exchange or price is None:
            return jsonify({"error": "symbol, exchange, and price are required"}), 400
        try:
            price_value = float(price)
        except (TypeError, ValueError):
            return jsonify({"error": "invalid price"}), 400
        ts_value: int | None
        try:
            ts_value = int(timestamp_ms) if timestamp_ms is not None else None
        except (TypeError, ValueError):
            ts_value = None
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(quote_store.upsert(
            symbol.upper(),
            exchange.lower(),
            price_value,
            timestamp_ms=ts_value,
            native_symbol=native_symbol,
        ))
        return jsonify({"status": "ok"})

    @app.route("/internal/telegram/status")
    def telegram_status() -> Any:
        if not notifier:
            return jsonify({"enabled": False})
        return jsonify({"enabled": notifier.is_enabled()})

    @app.route("/internal/telegram/toggle", methods=["POST"])
    def telegram_toggle() -> Any:
        if not notifier:
            return jsonify({"error": "notifier unavailable"}), 503
        data = request.get_json(silent=True) or {}
        if "enabled" not in data:
            return jsonify({"error": "enabled flag required"}), 400
        notifier.set_enabled(bool(data["enabled"]))
        return jsonify({"enabled": notifier.is_enabled()})

    if arbitrage_engine:

        def emit_loop() -> None:
            log.info("Starting WebSocket emit loop")
            import asyncio as aio
            loop = aio.new_event_loop()
            aio.set_event_loop(loop)
            try:
                while True:
                    opportunities = loop.run_until_complete(arbitrage_engine.get_latest())
                    payload = [asdict(opp) for opp in opportunities]
                    if payload:
                        log.debug("Emitting %d opportunities via WebSocket", len(payload))
                    socketio.emit("opportunities", payload)
                    import time
                    time.sleep(1)
            except Exception as e:
                log.exception("Error in emit loop: %s", e)
            finally:
                loop.close()

        socketio.start_background_task(emit_loop)

    return app, socketio

