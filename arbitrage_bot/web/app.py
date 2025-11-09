from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from typing import Any

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO

from arbitrage_bot.config import Settings, load_settings, save_filtering_config
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
    aggregator: "QuoteAggregator | None" = None,
) -> tuple[Flask, SocketIO]:
    app = Flask(__name__, static_folder="static", template_folder="templates")
    # Сохраняем ссылки на settings и engine для использования в роутах
    app.config["SETTINGS"] = settings
    app.config["ARBITRAGE_ENGINE"] = arbitrage_engine
    app.config["AGGREGATOR"] = aggregator
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
        current_engine = app.config.get("ARBITRAGE_ENGINE") or arbitrage_engine
        if not current_engine:
            return jsonify([])
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        opportunities = loop.run_until_complete(current_engine.get_latest())
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
                "is_stable": opp.is_stable,
            }
            for opp in opportunities
        ]
        log.debug("API /ranking: returning %d opportunities", len(payload))
        return jsonify(payload)

    @app.route("/api/filtering-config", methods=["GET", "POST"])
    def filtering_config() -> Any:
        """Get or save filtering configuration."""
        current_settings = app.config.get("SETTINGS") or settings
        current_engine = app.config.get("ARBITRAGE_ENGINE") or arbitrage_engine
        
        if request.method == "POST":
            # Сохранение конфига
            if not current_settings:
                return jsonify({"error": "Settings not available"}), 500
            
            try:
                data = request.get_json()
                if not data:
                    return jsonify({"error": "No data provided"}), 400
                
                # Валидация данных
                filtering_data = {
                    "same_coin_ratio": float(data.get("same_coin_ratio", current_settings.filtering.same_coin_ratio)),
                    "likely_same_coin_ratio": float(data.get("likely_same_coin_ratio", current_settings.filtering.likely_same_coin_ratio)),
                    "different_coin_ratio": float(data.get("different_coin_ratio", current_settings.filtering.different_coin_ratio)),
                    "min_price_threshold": float(data.get("min_price_threshold", current_settings.filtering.min_price_threshold)),
                    "price_ratio_threshold": float(data.get("price_ratio_threshold", current_settings.filtering.price_ratio_threshold)),
                    "stable_window_minutes": float(data.get("stable_window_minutes", current_settings.filtering.stable_window_minutes)),
                    "price_diff_suspicious": float(data.get("price_diff_suspicious", current_settings.filtering.price_diff_suspicious)),
                    "price_diff_threshold": float(data.get("price_diff_threshold", current_settings.filtering.price_diff_threshold)),
                    "price_diff_aggressive": float(data.get("price_diff_aggressive", current_settings.filtering.price_diff_aggressive)),
                }
                
                # Сохраняем в файл
                save_filtering_config(filtering_data)
                
                # Перезагружаем настройки
                new_settings = load_settings()
                
                # Обновляем настройки в engine без перезапуска
                if current_engine:
                    current_engine.reload_settings(new_settings)
                
                # Обновляем настройки в app.config для следующего запроса
                app.config["SETTINGS"] = new_settings
                
                log.info("Filtering config saved and reloaded")
                
                return jsonify({"success": True, "message": "Настройки сохранены и применены"})
            except Exception as e:
                log.exception("Error saving filtering config: %s", e)
                return jsonify({"error": str(e)}), 500
        
        # GET запрос - возвращаем текущие настройки
        if not current_settings:
            return jsonify({})
        return jsonify({
            # Backend параметры
            "same_coin_ratio": current_settings.filtering.same_coin_ratio,
            "likely_same_coin_ratio": current_settings.filtering.likely_same_coin_ratio,
            "different_coin_ratio": current_settings.filtering.different_coin_ratio,
            "min_price_threshold": current_settings.filtering.min_price_threshold,
            "price_ratio_threshold": current_settings.filtering.price_ratio_threshold,
            "stable_window_minutes": current_settings.filtering.stable_window_minutes,
            # Frontend параметры
            "price_diff_suspicious": current_settings.filtering.price_diff_suspicious,
            "price_diff_threshold": current_settings.filtering.price_diff_threshold,
            "price_diff_aggressive": current_settings.filtering.price_diff_aggressive,
        })

    @app.route("/api/exchange-status")
    def exchange_status() -> Any:
        """Get status of all exchanges."""
        current_aggregator = app.config.get("AGGREGATOR") or aggregator
        if not current_aggregator:
            return jsonify({})
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        statuses = loop.run_until_complete(current_aggregator.get_exchange_status())
        payload = {
            name: {
                "name": status.name,
                "connected": status.connected,
                "last_update_ms": status.last_update_ms,
                "quote_count": status.quote_count,
                "error_count": status.error_count,
                "last_error": status.last_error,
            }
            for name, status in statuses.items()
        }
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
        
        @socketio.on("connect")
        def handle_connect() -> None:
            """Send initial data immediately when client connects."""
            log.debug("Client connected, sending initial data")
            import asyncio as aio
            try:
                loop = aio.get_event_loop()
            except RuntimeError:
                loop = aio.new_event_loop()
                aio.set_event_loop(loop)
            
            # Send opportunities immediately
            opportunities = loop.run_until_complete(arbitrage_engine.get_latest())
            payload = [asdict(opp) for opp in opportunities]
            socketio.emit("opportunities", payload)
            log.debug("Sent %d initial opportunities to client", len(payload))
            
            # Send exchange status immediately
            if aggregator:
                try:
                    statuses = loop.run_until_complete(aggregator.get_exchange_status())
                    status_payload = {
                        name: {
                            "name": status.name,
                            "connected": status.connected,
                            "last_update_ms": status.last_update_ms,
                            "quote_count": status.quote_count,
                            "error_count": status.error_count,
                            "last_error": status.last_error,
                        }
                        for name, status in statuses.items()
                    }
                    socketio.emit("exchange_status", status_payload)
                    log.debug("Sent initial exchange status to client")
                except Exception as e:
                    log.debug("Error sending initial exchange status: %s", e)

        def emit_loop() -> None:
            log.info("Starting WebSocket emit loop")
            import asyncio as aio
            loop = aio.new_event_loop()
            aio.set_event_loop(loop)
            import time
            first_emit = True
            try:
                while True:
                    opportunities = loop.run_until_complete(arbitrage_engine.get_latest())
                    payload = [asdict(opp) for opp in opportunities]
                    if payload:
                        log.debug("Emitting %d opportunities via WebSocket", len(payload))
                    else:
                        log.debug("Emitting empty opportunities list (no opportunities found yet)")
                    socketio.emit("opportunities", payload)
                    
                    # Emit exchange status if aggregator is available
                    if aggregator:
                        try:
                            statuses = loop.run_until_complete(aggregator.get_exchange_status())
                            status_payload = {
                                name: {
                                    "name": status.name,
                                    "connected": status.connected,
                                    "last_update_ms": status.last_update_ms,
                                    "quote_count": status.quote_count,
                                    "error_count": status.error_count,
                                    "last_error": status.last_error,
                                }
                                for name, status in statuses.items()
                            }
                            socketio.emit("exchange_status", status_payload)
                        except Exception as e:
                            log.debug("Error emitting exchange status: %s", e)
                    
                    # Первая отправка без задержки, затем с интервалом 1 секунда
                    if not first_emit:
                        time.sleep(1)
                    else:
                        first_emit = False
            except Exception as e:
                log.exception("Error in emit loop: %s", e)
            finally:
                loop.close()

        socketio.start_background_task(emit_loop)

    return app, socketio

