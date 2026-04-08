namespace TradePulse.Infrastructure.Ingestion;

// Wire format (length-prefixed binary):
// [4 bytes: total payload length] [payload bytes: UTF-8 JSON tick]
//
// This is intentionally simple so the simulator and server share the same framing logic.
internal static class TickFraming
{
    public const int HeaderSize = 4; // int32 payload length
}
