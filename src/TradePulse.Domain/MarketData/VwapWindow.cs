namespace TradePulse.Domain.MarketData;

public sealed record VwapWindow(
    string Symbol,
    decimal Vwap,
    decimal TotalVolume,
    DateTimeOffset WindowStart,
    DateTimeOffset WindowEnd,
    int TickCount);
