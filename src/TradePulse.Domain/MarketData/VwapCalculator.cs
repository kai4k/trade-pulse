namespace TradePulse.Domain.MarketData;

/// <summary>
/// Computes the Volume-Weighted Average Price (VWAP) for a closed time window.
///
/// Formula: VWAP = Σ(Mid_i × Volume_i) / Σ(Volume_i)
///
/// This is a pure static class — no I/O, no state, no dependencies.
/// All behaviour is covered by VwapCalculatorTests.
/// </summary>
public static class VwapCalculator
{
    /// <summary>
    /// Computes a <see cref="VwapWindow"/> from a non-empty list of ticks within a time window.
    /// </summary>
    /// <param name="symbol">The ticker symbol all ticks belong to.</param>
    /// <param name="ticks">The ticks that fell within the window. Must be non-empty.</param>
    /// <param name="windowStart">Inclusive start of the time window.</param>
    /// <param name="windowEnd">Exclusive end of the time window.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="ticks"/> is empty.</exception>
    public static VwapWindow Compute(
        string symbol,
        IList<Tick> ticks,
        DateTimeOffset windowStart,
        DateTimeOffset windowEnd)
    {
        if (ticks.Count == 0)
            throw new ArgumentException("Cannot compute VWAP for an empty window.", nameof(ticks));

        decimal totalNotional = 0m; // Σ(mid × volume)
        decimal totalVolume   = 0m; // Σ(volume)

        foreach (var tick in ticks)
        {
            totalNotional += tick.Mid * tick.Volume;
            totalVolume   += tick.Volume;
        }

        return new VwapWindow(
            Symbol:      symbol,
            Vwap:        totalNotional / totalVolume,
            TotalVolume: totalVolume,
            WindowStart: windowStart,
            WindowEnd:   windowEnd,
            TickCount:   ticks.Count);
    }
}
