from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from typing import Any

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO

from arbitrage_bot.config import Settings, load_settings, save_filtering_config, save_profit_config, save_exchange_config
from arbitrage_bot.services.arbitrage_engine import ArbitrageEngine
from arbitrage_bot.services.market_discovery import MarketDiscoveryService
from arbitrage_bot.services.quote_aggregator import QuoteAggregator
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
    # Сохраняем ссылку на основной event loop (будет установлена в AppRunner)
    app.config["MAIN_LOOP"] = None
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
                log.info("Filtering config saved to file: %s", filtering_data)
                
                # Перезагружаем настройки
                new_settings = load_settings()
                log.info("Settings reloaded from file")
                
                # Обновляем настройки в engine без перезапуска
                if current_engine:
                    current_engine.reload_settings(new_settings)
                    log.info("ArbitrageEngine settings updated")
                else:
                    log.warning("ArbitrageEngine not available, settings not applied")
                
                # Обновляем настройки в app.config для следующего запроса
                app.config["SETTINGS"] = new_settings
                app.config["ARBITRAGE_ENGINE"] = current_engine  # Обновляем ссылку на engine с новыми настройками
                log.info("App config updated with new settings")
                
                return jsonify({
                    "success": True, 
                    "message": "Настройки сохранены и применены",
                    "applied": current_engine is not None
                })
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

    @app.route("/api/profit-config", methods=["GET", "POST"])
    def profit_config() -> Any:
        """Get or save profit calculation configuration."""
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
                profit_data = {
                    "notional_usdt_default": float(data.get("notional_usdt_default", current_settings.notional_usdt_default)),
                    "slippage_bps": float(data.get("slippage_bps", current_settings.slippage_bps)),
                    "min_profit_usdt": float(data.get("min_profit_usdt", current_settings.thresholds.min_profit_usdt)),
                    "min_spread_pct": float(data.get("min_spread_pct", current_settings.thresholds.min_spread_pct)),
                }
                
                # Валидация
                if profit_data["notional_usdt_default"] < 1:
                    return jsonify({"error": "Notional USDT must be >= 1"}), 400
                if profit_data["slippage_bps"] < 0:
                    return jsonify({"error": "Slippage cannot be negative"}), 400
                if profit_data["min_profit_usdt"] < 0:
                    return jsonify({"error": "Min profit cannot be negative"}), 400
                if profit_data["min_spread_pct"] < 0:
                    return jsonify({"error": "Min spread cannot be negative"}), 400
                
                # Сохраняем в файл
                save_profit_config(profit_data)
                log.info("Profit config saved to file: %s", profit_data)
                
                # Перезагружаем настройки
                new_settings = load_settings()
                log.info("Settings reloaded from file")
                
                # Обновляем настройки в engine без перезапуска
                if current_engine:
                    current_engine.reload_settings(new_settings)
                    log.info("ArbitrageEngine settings updated")
                else:
                    log.warning("ArbitrageEngine not available, settings not applied")
                
                # Обновляем настройки в app.config для следующего запроса
                app.config["SETTINGS"] = new_settings
                app.config["ARBITRAGE_ENGINE"] = current_engine
                log.info("App config updated with new settings")
                
                return jsonify({
                    "success": True, 
                    "message": "Параметры расчета прибыли сохранены и применены",
                    "applied": current_engine is not None
                })
            except Exception as e:
                log.exception("Error saving profit config: %s", e)
                return jsonify({"error": str(e)}), 500
        
        # GET запрос - возвращаем текущие настройки
        if not current_settings:
            return jsonify({})
        return jsonify({
            "notional_usdt_default": current_settings.notional_usdt_default,
            "slippage_bps": current_settings.slippage_bps,
            "min_profit_usdt": current_settings.thresholds.min_profit_usdt,
            "min_spread_pct": current_settings.thresholds.min_spread_pct,
        })

    @app.route("/api/exchange-config", methods=["GET", "POST"])
    def exchange_config() -> Any:
        """Get or save exchange enabled/disabled configuration."""
        current_settings = app.config.get("SETTINGS") or settings
        current_aggregator = app.config.get("AGGREGATOR") or aggregator
        current_socketio = socketio
        
        if request.method == "POST":
            # Сохранение конфига
            if not current_settings:
                return jsonify({"error": "Settings not available"}), 500
            
            try:
                data = request.get_json()
                if not data:
                    return jsonify({"error": "No data provided"}), 400
                
                # Валидация данных - exchange_enabled должен быть словарем
                exchange_enabled = data.get("exchange_enabled", {})
                if not isinstance(exchange_enabled, dict):
                    return jsonify({"error": "exchange_enabled must be a dictionary"}), 400
                
                # Валидация значений (должны быть bool)
                validated_config = {}
                for exchange_name, enabled in exchange_enabled.items():
                    if exchange_name not in current_settings.exchanges:
                        log.warning("Unknown exchange in config: %s", exchange_name)
                        continue
                    validated_config[exchange_name] = bool(enabled)
                
                # Сохраняем в файл
                save_exchange_config(validated_config)
                log.info("Exchange config saved to file: %s", validated_config)
                
                # Перезагружаем настройки
                new_settings = load_settings()
                log.info("Settings reloaded from file")
                
                # Обновляем aggregator используя основной event loop приложения
                if current_aggregator:
                    main_loop = app.config.get("MAIN_LOOP")
                    if main_loop and main_loop.is_running():
                        # Используем run_coroutine_threadsafe для выполнения в основном loop
                        async def update_aggregator():
                            try:
                                # Останавливаем отключенные биржи и запускаем включенные
                                for exchange_name, enabled in validated_config.items():
                                    if not enabled:
                                        # Останавливаем только если биржа запущена
                                        if exchange_name in current_aggregator._exchange_tasks:
                                            await current_aggregator.stop_exchange(exchange_name)
                                    else:
                                        # Запускаем только если биржа не запущена
                                        if exchange_name not in current_aggregator._exchange_tasks:
                                            await current_aggregator.start_exchange(exchange_name)
                                current_aggregator.update_exchange_enabled(validated_config)
                                log.info("QuoteAggregator updated with new exchange config")
                            except Exception as e:
                                log.exception("Error updating aggregator: %s", e)
                                raise
                        
                        # Запускаем в основном loop через run_coroutine_threadsafe
                        import asyncio
                        future = asyncio.run_coroutine_threadsafe(update_aggregator(), main_loop)
                        # Не ждем завершения, чтобы не блокировать Flask
                        log.info("QuoteAggregator update scheduled in main event loop")
                    else:
                        # Если основной loop недоступен, используем фоновый поток
                        def update_aggregator_sync():
                            """Синхронная обертка для обновления aggregator."""
                            import asyncio
                            new_loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(new_loop)
                            try:
                                async def update_aggregator():
                                    try:
                                        for exchange_name, enabled in validated_config.items():
                                            if not enabled:
                                                if exchange_name in current_aggregator._exchange_tasks:
                                                    await current_aggregator.stop_exchange(exchange_name)
                                            else:
                                                if exchange_name not in current_aggregator._exchange_tasks:
                                                    await current_aggregator.start_exchange(exchange_name)
                                        current_aggregator.update_exchange_enabled(validated_config)
                                        log.info("QuoteAggregator updated with new exchange config")
                                    except Exception as e:
                                        log.exception("Error updating aggregator: %s", e)
                                        raise
                                
                                new_loop.run_until_complete(update_aggregator())
                            except Exception as e:
                                log.exception("Error in update_aggregator_sync: %s", e)
                            finally:
                                new_loop.close()
                        
                        current_socketio.start_background_task(update_aggregator_sync)
                        log.info("QuoteAggregator update scheduled in background thread")
                else:
                    log.warning("QuoteAggregator not available, settings not applied")
                
                # Обновляем настройки в app.config для следующего запроса
                app.config["SETTINGS"] = new_settings
                app.config["AGGREGATOR"] = current_aggregator
                log.info("App config updated with new settings")
                
                return jsonify({
                    "success": True, 
                    "message": "Настройки бирж сохранены и применены",
                    "applied": current_aggregator is not None
                })
            except Exception as e:
                log.exception("Error saving exchange config: %s", e)
                return jsonify({"error": str(e)}), 500
        
        # GET запрос - возвращаем текущие настройки
        if not current_settings:
            return jsonify({})
        return jsonify({
            "exchange_enabled": dict(current_settings.exchange_enabled),
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

    @app.route("/api/test-exchange/<exchange_name>", methods=["POST"])
    def test_exchange(exchange_name: str) -> Any:
        """Test exchange connection and return detailed results."""
        from arbitrage_bot.bootstrap import create_adapters
        from arbitrage_bot.core import HttpClientFactory
        
        current_settings = app.config.get("SETTINGS") or settings
        if not current_settings:
            return jsonify({"error": "Settings not available"}), 500
        
        if exchange_name not in current_settings.exchanges:
            return jsonify({"error": f"Exchange {exchange_name} not configured"}), 404
        
        try:
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            async def run_test():
                http_factory = HttpClientFactory()
                try:
                    # Создаем адаптер для тестирования
                    adapters = create_adapters(current_settings, http_factory)
                    adapter = next((a for a in adapters if a.name == exchange_name), None)
                    
                    if not adapter:
                        return {"error": f"Adapter for {exchange_name} not found"}
                    
                    results = {
                        "exchange": exchange_name,
                        "tests": [],
                        "success": True,
                        "summary": ""
                    }
                    
                    # Тест 1: fetch_markets
                    try:
                        markets = await adapter.fetch_markets()
                        results["tests"].append({
                            "name": "fetch_markets",
                            "success": True,
                            "message": f"Получено {len(markets)} рынков",
                            "details": {
                                "markets_count": len(markets),
                                "sample_symbols": [m.symbol for m in markets[:5]] if markets else []
                            }
                        })
                    except Exception as e:
                        results["success"] = False
                        results["tests"].append({
                            "name": "fetch_markets",
                            "success": False,
                            "message": f"Ошибка: {str(e)}",
                            "details": {"error": str(e)}
                        })
                    
                    # Тест 2: quote_stream (если есть рынки)
                    if results["tests"][0]["success"] and results["tests"][0]["details"]["markets_count"] > 0:
                        test_symbols = [markets[0].symbol, markets[1].symbol if len(markets) > 1 else markets[0].symbol]
                        try:
                            quote_count = 0
                            async for quote in adapter.quote_stream(test_symbols):
                                quote_count += 1
                                if quote_count >= 2:
                                    break
                            
                            if quote_count > 0:
                                results["tests"].append({
                                    "name": "quote_stream",
                                    "success": True,
                                    "message": f"Получено {quote_count} котировок",
                                    "details": {
                                        "quotes_received": quote_count,
                                        "test_symbols": test_symbols
                                    }
                                })
                            else:
                                results["success"] = False
                                results["tests"].append({
                                    "name": "quote_stream",
                                    "success": False,
                                    "message": "Не получено ни одной котировки",
                                    "details": {"test_symbols": test_symbols}
                                })
                        except Exception as e:
                            results["success"] = False
                            results["tests"].append({
                                "name": "quote_stream",
                                "success": False,
                                "message": f"Ошибка: {str(e)}",
                                "details": {"error": str(e)}
                            })
                    
                    # Формируем summary
                    passed = sum(1 for t in results["tests"] if t["success"])
                    total = len(results["tests"])
                    results["summary"] = f"Пройдено тестов: {passed}/{total}"
                    
                    await adapter.close()
                    await http_factory.close()
                    return results
                except Exception as e:
                    return {"error": str(e), "exchange": exchange_name}
            
            result = loop.run_until_complete(run_test())
            return jsonify(result)
        except Exception as e:
            log.exception("Error testing exchange %s: %s", exchange_name, e)
            return jsonify({"error": str(e), "exchange": exchange_name}), 500

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

