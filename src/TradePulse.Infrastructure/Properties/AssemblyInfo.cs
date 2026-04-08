using System.Runtime.CompilerServices;

// Grants the Infrastructure test project access to internal members (e.g. the
// TickStreamProcessor(IScheduler) constructor used for virtual-time TDD).
[assembly: InternalsVisibleTo("TradePulse.Infrastructure.Tests")]
