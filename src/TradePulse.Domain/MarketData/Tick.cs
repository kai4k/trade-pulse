namespace TradePulse.Domain.MarketData;

public sealed record Tick(
    string Symbol,
    decimal Bid,
    decimal Ask,
    long Volume,
    DateTimeOffset Timestamp)
{
    public decimal Mid => (Bid + Ask) / 2m;
    public decimal Spread => Ask - Bid;
}
