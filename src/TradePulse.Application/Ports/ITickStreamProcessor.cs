using TradePulse.Domain.MarketData;

namespace TradePulse.Application.Ports;

/// <summary>
/// Applies Rx.NET stream operators to a raw tick stream, producing aggregated market data.
/// Implementations live in Infrastructure; callers never see <c>System.Reactive</c> internals.
/// </summary>
public interface ITickStreamProcessor
{
    /// <summary>
    /// Groups ticks by symbol, buffers them into non-overlapping time windows, and emits
    /// a <see cref="VwapWindow"/> for each symbol per window.  Windows with no ticks are
    /// suppressed — the output stream is sparse by design.
    /// </summary>
    /// <param name="source">The raw tick stream to process.</param>
    /// <param name="windowSize">Duration of each non-overlapping VWAP window.</param>
    IObservable<VwapWindow> ComputeVwap(IObservable<Tick> source, TimeSpan windowSize);
}
