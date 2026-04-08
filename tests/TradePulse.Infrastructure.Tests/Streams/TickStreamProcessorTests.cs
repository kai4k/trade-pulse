using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using FluentAssertions;
using Microsoft.Reactive.Testing;
using TradePulse.Domain.MarketData;
using TradePulse.Infrastructure.Streams;

namespace TradePulse.Infrastructure.Tests.Streams;

/// <summary>
/// TDD — RED phase: tests define the TickStreamProcessor contract before the class exists.
///
/// Key design decisions captured here:
///   • TestScheduler provides virtual time — no real timers, no Thread.Sleep, deterministic.
///   • Subject&lt;Tick&gt; acts as the controllable source (simulates a live channel drain).
///   • AdvanceBy(windowSize.Ticks) triggers Buffer window expiry synchronously.
///   • Empty windows are suppressed — the processor never emits a VwapWindow with 0 ticks.
///   • Multi-symbol streams produce independent, per-symbol VwapWindows.
/// </summary>
public sealed class TickStreamProcessorTests
{
    private static readonly DateTimeOffset AnyTime = DateTimeOffset.UtcNow;

    // Helpers ─────────────────────────────────────────────────────────────────

    private static Tick AaplTick(decimal bid, decimal ask, long volume = 1_000) =>
        new("AAPL", bid, ask, volume, AnyTime);

    private static Tick MsftTick(decimal bid, decimal ask, long volume = 500) =>
        new("MSFT", bid, ask, volume, AnyTime);

    // ── Single window emits one VwapWindow ───────────────────────────────────

    [Fact]
    public void ComputeVwap_WithTicksInOneWindow_EmitsExactlyOneWindow()
    {
        var scheduler  = new TestScheduler();
        var windowSize = TimeSpan.FromTicks(100);
        var processor  = new TickStreamProcessor(scheduler);
        var source     = new Subject<Tick>();
        var results    = new List<VwapWindow>();

        using var _ = processor.ComputeVwap(source, windowSize).Subscribe(results.Add);

        source.OnNext(AaplTick(184.99m, 185.01m, volume: 1_000));
        source.OnNext(AaplTick(185.09m, 185.11m, volume: 2_000));

        scheduler.AdvanceBy(windowSize.Ticks); // close the buffer window

        results.Should().HaveCount(1);
        results[0].Symbol.Should().Be("AAPL");
        results[0].TickCount.Should().Be(2);
        results[0].TotalVolume.Should().Be(3_000m);
    }

    // ── VWAP math is correct end-to-end ──────────────────────────────────────

    [Fact]
    public void ComputeVwap_CalculatesCorrectVolumeWeightedAveragePrice()
    {
        // Mid1 = 185.00 (vol 1_000), Mid2 = 190.00 (vol 4_000)
        // VWAP = (185×1000 + 190×4000) / 5000 = 189.00
        var scheduler  = new TestScheduler();
        var windowSize = TimeSpan.FromTicks(100);
        var processor  = new TickStreamProcessor(scheduler);
        var source     = new Subject<Tick>();
        var results    = new List<VwapWindow>();

        using var _ = processor.ComputeVwap(source, windowSize).Subscribe(results.Add);

        source.OnNext(AaplTick(184.99m, 185.01m, volume: 1_000));
        source.OnNext(AaplTick(189.99m, 190.01m, volume: 4_000));

        scheduler.AdvanceBy(windowSize.Ticks);

        results.Should().HaveCount(1);
        results[0].Vwap.Should().Be(189.00m);
    }

    // ── Two consecutive windows emit independently ────────────────────────────

    [Fact]
    public void ComputeVwap_WithTicksInTwoConsecutiveWindows_EmitsTwoWindows()
    {
        var scheduler  = new TestScheduler();
        var windowSize = TimeSpan.FromTicks(100);
        var processor  = new TickStreamProcessor(scheduler);
        var source     = new Subject<Tick>();
        var results    = new List<VwapWindow>();

        using var _ = processor.ComputeVwap(source, windowSize).Subscribe(results.Add);

        // Window 1
        source.OnNext(AaplTick(184.99m, 185.01m, volume: 1_000));
        scheduler.AdvanceBy(windowSize.Ticks);

        // Window 2
        source.OnNext(AaplTick(185.49m, 185.51m, volume: 2_000));
        scheduler.AdvanceBy(windowSize.Ticks);

        results.Should().HaveCount(2);
        results[0].TickCount.Should().Be(1);
        results[1].TickCount.Should().Be(1);
        results[1].TotalVolume.Should().Be(2_000m);
    }

    // ── Empty windows are swallowed — no noise in the output stream ───────────

    [Fact]
    public void ComputeVwap_WhenWindowContainsNoTicks_EmitsNoWindowForThatPeriod()
    {
        var scheduler  = new TestScheduler();
        var windowSize = TimeSpan.FromTicks(100);
        var processor  = new TickStreamProcessor(scheduler);
        var source     = new Subject<Tick>();
        var results    = new List<VwapWindow>();

        using var _ = processor.ComputeVwap(source, windowSize).Subscribe(results.Add);

        // Window 1: 1 tick → emits
        source.OnNext(AaplTick(184.99m, 185.01m));
        scheduler.AdvanceBy(windowSize.Ticks);

        // Window 2: no ticks → must NOT emit
        scheduler.AdvanceBy(windowSize.Ticks);

        // Window 3: 1 tick → emits
        source.OnNext(AaplTick(185.49m, 185.51m));
        scheduler.AdvanceBy(windowSize.Ticks);

        results.Should().HaveCount(2, because: "empty windows must be suppressed");
    }

    // ── Multiple symbols are processed independently ──────────────────────────

    [Fact]
    public void ComputeVwap_WithMultipleSymbols_EmitsOneWindowPerSymbol()
    {
        var scheduler  = new TestScheduler();
        var windowSize = TimeSpan.FromTicks(100);
        var processor  = new TickStreamProcessor(scheduler);
        var source     = new Subject<Tick>();
        var results    = new List<VwapWindow>();

        using var _ = processor.ComputeVwap(source, windowSize).Subscribe(results.Add);

        source.OnNext(AaplTick(184.99m, 185.01m, volume: 1_000));
        source.OnNext(MsftTick(414.99m, 415.01m, volume: 500));

        scheduler.AdvanceBy(windowSize.Ticks);

        results.Should().HaveCount(2);
        results.Should().ContainSingle(w => w.Symbol == "AAPL")
            .Which.TotalVolume.Should().Be(1_000m);
        results.Should().ContainSingle(w => w.Symbol == "MSFT")
            .Which.TotalVolume.Should().Be(500m);
    }

    // ── Each symbol's VWAP is independent — one symbol's volume doesn't pollute another's

    [Fact]
    public void ComputeVwap_SymbolsDoNotCrossContaminateVwapCalculation()
    {
        var scheduler  = new TestScheduler();
        var windowSize = TimeSpan.FromTicks(100);
        var processor  = new TickStreamProcessor(scheduler);
        var source     = new Subject<Tick>();
        var results    = new List<VwapWindow>();

        using var _ = processor.ComputeVwap(source, windowSize).Subscribe(results.Add);

        // AAPL: mid 185.00, vol 1_000 → VWAP 185.00
        // MSFT: mid 415.00, vol 9_000 → VWAP 415.00 (high volume must not bleed into AAPL)
        source.OnNext(AaplTick(184.99m, 185.01m, volume: 1_000));
        source.OnNext(MsftTick(414.99m, 415.01m, volume: 9_000));

        scheduler.AdvanceBy(windowSize.Ticks);

        var aaplWindow = results.Single(w => w.Symbol == "AAPL");
        var msftWindow = results.Single(w => w.Symbol == "MSFT");

        aaplWindow.Vwap.Should().Be(185.00m);
        msftWindow.Vwap.Should().Be(415.00m);
    }

    // ── Errors are forwarded to the subscriber ────────────────────────────────

    [Fact]
    public void ComputeVwap_WhenSourceErrors_ForwardsErrorToSubscriber()
    {
        var scheduler  = new TestScheduler();
        var windowSize = TimeSpan.FromTicks(100);
        var processor  = new TickStreamProcessor(scheduler);
        var source     = new Subject<Tick>();
        Exception? caughtError = null;

        using var _ = processor.ComputeVwap(source, windowSize)
            .Subscribe(_ => { }, ex => caughtError = ex);

        var boom = new InvalidOperationException("upstream failure");
        source.OnError(boom);

        caughtError.Should().BeSameAs(boom);
    }
}
