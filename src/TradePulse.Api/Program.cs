using TradePulse.Application.Ports;
using TradePulse.Infrastructure.Ingestion;
using TradePulse.Infrastructure.Streams;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

// Ingestion
builder.Services.AddSingleton<TickIngestionServer>();
builder.Services.AddSingleton<ITickIngestionServer>(sp => sp.GetRequiredService<TickIngestionServer>());
builder.Services.AddSingleton<TickSimulatorClient>();

// Stream processing (Rx.NET — production uses DefaultScheduler via public ctor)
builder.Services.AddSingleton<ITickStreamProcessor, TickStreamProcessor>();

// Hosted service: orchestrates ingestion + Rx pipeline
builder.Services.AddHostedService<TickPipelineService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseHttpsRedirection();
app.Run();
