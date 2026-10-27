using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Ordering.Api.Infrastructure;
using Ordering.Api.Orders;
using Ordering.Infrastructure.Persistence;

var builder = WebApplication.CreateBuilder(args);

// A 100-line order is a few KB; a far larger body is a bug or an attack and must not cost CPU.
builder.WebHost.ConfigureKestrel(kestrel => kestrel.Limits.MaxRequestBodySize = 64 * 1024);

builder.Services.AddOrderingDbContextPool(
    builder.Configuration.GetConnectionString("Ordering")
    ?? throw new InvalidOperationException("Connection string 'Ordering' is not configured."));
builder.Services.AddSingleton(TimeProvider.System);

builder.Services.ConfigureHttpJsonOptions(json =>
{
    json.SerializerOptions.TypeInfoResolverChain.Insert(0, ApiJsonContext.Default);
    // ASP.NET Core's web defaults accept quoted numbers ("2"); treat that as the client error it is.
    json.SerializerOptions.NumberHandling = JsonNumberHandling.Strict;
});
builder.Services.AddValidation();
builder.Services.AddProblemDetails();
// Surface body-binding failures as exceptions in every environment, so one handler formats them consistently.
builder.Services.Configure<RouteHandlerOptions>(routing => routing.ThrowOnBadRequest = true);
builder.Services.AddExceptionHandler<BadRequestExceptionHandler>();
builder.Services.AddExceptionHandler<TransientDatabaseExceptionHandler>();
builder.Services.AddLoadShedding(builder.Configuration);
builder.Services.AddOpenApi();
builder.Services.AddHealthChecks().AddDbContextCheck<OrderingDbContext>("database", tags: ["ready"]);

var app = builder.Build();

app.UseExceptionHandler();
app.UseStatusCodePages();
app.UseRateLimiter();

app.MapOpenApi();
app.UseSwaggerUI(swagger => swagger.SwaggerEndpoint("/openapi/v1.json", "Ordering API v1"));
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false });
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") });
app.MapOrderEndpoints();

app.Run();

public partial class Program;
