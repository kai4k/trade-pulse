using System.Reactive.Concurrency;
using System.Reactive.Linq;
using TradePulse.Application.Ports;
using TradePulse.Domain.MarketData;

namespace TradePulse.Infrastructure.Streams;

/// <summary>
/// Rx.NET implementation of <see cref="ITickStreamProcessor"/>.
///
/// Pipeline per call to <see cref="ComputeVwap"/>:
///   source
///     → GroupBy(symbol)           — one inner stream per symbol
///     → Buffer(windowSize)        — non-overlapping time windows driven by <paramref name="scheduler"/>
///     → Where(buffer !empty)      — suppress silent windows (no ticks arrived)
///     → Select(VwapCalculator)    — pure-domain VWAP computation, zero allocation on hot path
///     → Merge                     — flatten all per-symbol streams back into one output stream
///
/// Scheduler injection:
///   Production uses <see cref="DefaultScheduler.Instance"/> (real wall-clock timers).
///   Tests inject a <see cref="Microsoft.Reactive.Testing.TestScheduler"/> for deterministic,
///   zero-wait virtual-time execution.  This avoids Thread.Sleep in tests entirely.
/// </summary>
public sealed class TickStreamProcessor : ITickStreamProcessor
{
    private readonly IScheduler _scheduler;

    /// <summary>
    /// Production constructor — uses real wall-clock timers.
    /// </summary>
    public TickStreamProcessor() : this(DefaultScheduler.Instance) { }

    /// <summary>
    /// Test constructor — accepts any <see cref="IScheduler"/> for virtual-time control.
    /// Internal visibility is intentional: only <c>TradePulse.Infrastructure.Tests</c>
    /// should instantiate this overload (via <see cref="InternalsVisibleToAttribute"/>).
    /// </summary>
    internal TickStreamProcessor(IScheduler scheduler)
    {
        _scheduler = scheduler;
    }

    /// <inheritdoc/>
    public IObservable<VwapWindow> ComputeVwap(IObservable<Tick> source, TimeSpan windowSize) =>
        source
            .GroupBy(tick => tick.Symbol)
            .SelectMany(symbolGroup =>
                symbolGroup
                    .Buffer(windowSize, _scheduler)
                    .Where(buffer => buffer.Count > 0)
                    .Select(buffer =>
                    {
                        // _scheduler.Now is the virtual (or real) time at window close.
                        // windowStart is derived so callers get accurate window boundaries.
                        var windowEnd   = _scheduler.Now;
                        var windowStart = windowEnd - windowSize;
                        return VwapCalculator.Compute(symbolGroup.Key, buffer, windowStart, windowEnd);
                    }));
}
