class ArbitrageError(Exception):
    """Base error for the arbitrage bot."""


class ExchangeError(ArbitrageError):
    """Raised when an exchange adapter fails."""


class DiscoveryError(ArbitrageError):
    """Raised for market discovery issues."""


class AggregationError(ArbitrageError):
    """Raised when quote aggregation fails."""


class NotificationError(ArbitrageError):
    """Raised when telegram notifications fail."""

