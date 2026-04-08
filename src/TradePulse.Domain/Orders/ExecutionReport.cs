namespace TradePulse.Domain.Orders;

public sealed record ExecutionReport(
    string ExecId,
    string OrderId,
    string Symbol,
    OrderSide Side,
    ExecutionType ExecType,
    OrderStatus OrderStatus,
    decimal OrderQuantity,
    decimal CumulativeQuantity,
    decimal LeavesQuantity,
    decimal? LastPrice,
    decimal? LastQuantity,
    decimal? AvgPrice,
    DateTimeOffset TransactTime);

public enum ExecutionType
{
    New,
    PartialFill,
    Fill,
    Cancelled,
    Rejected,
    PendingNew
}
