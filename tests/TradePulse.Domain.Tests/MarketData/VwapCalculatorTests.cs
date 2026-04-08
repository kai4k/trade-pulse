using FluentAssertions;
using TradePulse.Domain.MarketData;

namespace TradePulse.Domain.Tests.MarketData;

/// <summary>
/// TDD — RED phase: these tests were written before VwapCalculator existed.
/// They define the mathematical contract: VWAP = Σ(Mid_i × Volume_i) / Σ(Volume_i).
/// </summary>
public sealed class VwapCalculatorTests
{
    private static readonly DateTimeOffset AnyTime = new(2025, 6, 1, 9, 30, 0, TimeSpan.Zero);

    // ── Single tick ──────────────────────────────────────────────────────────

    [Fact]
    public void Compute_WithSingleTick_VwapEqualsMidPrice()
    {
        // Mid = (184.99 + 185.01) / 2 = 185.00
        var tick = new Tick("AAPL", 184.99m, 185.01m, Volume: 1_000, AnyTime);

        var result = VwapCalculator.Compute("AAPL", [tick], AnyTime, AnyTime);

        result.Vwap.Should().Be(185.00m);
        result.TotalVolume.Should().Be(1_000m);
        result.TickCount.Should().Be(1);
        result.Symbol.Should().Be("AAPL");
    }

    // ── Equal volumes → simple average of mid-prices ─────────────────────────

    [Fact]
    public void Compute_WithEqualVolumes_VwapIsSimpleAverageOfMids()
    {
        // Mid1 = 185.00, Mid2 = 186.00, both volume 1_000
        // VWAP = (185.00×1000 + 186.00×1000) / 2000 = 185.50
        var ticks = new[]
        {
            new Tick("AAPL", 184.99m, 185.01m, Volume: 1_000, AnyTime),
            new Tick("AAPL", 185.99m, 186.01m, Volume: 1_000, AnyTime.AddSeconds(1))
        };

        var result = VwapCalculator.Compute("AAPL", ticks, AnyTime, AnyTime.AddSeconds(1));

        result.Vwap.Should().Be(185.50m);
        result.TotalVolume.Should().Be(2_000m);
        result.TickCount.Should().Be(2);
    }

    // ── Unequal volumes → weighted toward the higher-volume tick ─────────────

    [Fact]
    public void Compute_WithUnequalVolumes_VwapIsVolumeWeightedTowardHigherVolumeTick()
    {
        // Mid1 = 185.00 (vol 1_000), Mid2 = 190.00 (vol 4_000)
        // VWAP = (185.00×1000 + 190.00×4000) / 5000
        //      = (185_000 + 760_000) / 5000
        //      = 945_000 / 5000 = 189.00
        var ticks = new[]
        {
            new Tick("AAPL", 184.99m, 185.01m, Volume: 1_000, AnyTime),
            new Tick("AAPL", 189.99m, 190.01m, Volume: 4_000, AnyTime.AddSeconds(1))
        };

        var result = VwapCalculator.Compute("AAPL", ticks, AnyTime, AnyTime.AddSeconds(1));

        result.Vwap.Should().Be(189.00m);
        result.TotalVolume.Should().Be(5_000m);
    }

    // ── Window boundaries are preserved exactly ───────────────────────────────

    [Fact]
    public void Compute_PreservesWindowBoundariesVerbatim()
    {
        var windowStart = new DateTimeOffset(2025, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var windowEnd   = windowStart.AddSeconds(5);
        var tick = new Tick("MSFT", 414.99m, 415.01m, Volume: 500, windowStart);

        var result = VwapCalculator.Compute("MSFT", [tick], windowStart, windowEnd);

        result.WindowStart.Should().Be(windowStart);
        result.WindowEnd.Should().Be(windowEnd);
        result.Symbol.Should().Be("MSFT");
    }

    // ── Guard: empty input is a programming error, not a business edge-case ───

    [Fact]
    public void Compute_WithEmptyTickList_ThrowsArgumentException()
    {
        var act = () => VwapCalculator.Compute("AAPL", [], AnyTime, AnyTime);

        act.Should().Throw<ArgumentException>()
            .WithParameterName("ticks")
            .WithMessage("*empty*");
    }

    // ── Tick count is surfaced for observability ──────────────────────────────

    [Fact]
    public void Compute_TickCountMatchesInputLength()
    {
        var ticks = new[]
        {
            new Tick("TSLA", 174.99m, 175.01m, Volume: 200, AnyTime),
            new Tick("TSLA", 175.49m, 175.51m, Volume: 300, AnyTime.AddSeconds(1)),
            new Tick("TSLA", 176.49m, 176.51m, Volume: 100, AnyTime.AddSeconds(2))
        };

        var result = VwapCalculator.Compute("TSLA", ticks, AnyTime, AnyTime.AddSeconds(2));

        result.TickCount.Should().Be(3);
    }
}
