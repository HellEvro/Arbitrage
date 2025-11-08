 # Arbitrage Bot

 Modular async crypto arbitrage bot targeting Bybit, MEXC, Bitget, OKX, and KuCoin.

 ## Quick start

 ```bash
 python -m venv .venv
 source .venv/bin/activate  # or .venv\Scripts\activate on Windows
 pip install -e ".[dev]"
 cp config/config.example.yaml config/config.yaml
 ```

 ## Components

 - `MarketDiscovery` — пересечение спотовых рынков
 - `ExchangeAdapters` — унифицированные адаптеры для бирж (REST/WS)
 - `QuoteAggregator` — агрегация котировок в память/Redis
 - `ArbitrageEngine` — расчёт спредов и прибыли
 - `WebUI` — Flask + SocketIO таблица рейтинга
 - `TelegramNotifier` — уведомления о топ-возможностях

 ## Development

 ```bash
 make lint
 make test
 ```

