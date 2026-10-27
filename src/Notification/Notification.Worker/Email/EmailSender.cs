using MailKit.Net.Smtp;
using Microsoft.Extensions.Options;
using MimeKit;
using Polly;
using Polly.CircuitBreaker;
using Polly.Registry;

namespace Notification.Worker.Email;

/// <param name="MessageId">Deterministic per (order, e-mail type): a re-sent duplicate carries the same Message-ID.</param>
public sealed record EmailMessage(string MessageId, string To, string Subject, string TextBody, string HtmlBody);

public interface IEmailSender
{
    /// <exception cref="PermanentEmailException">The server rejected the message for good (5xx) or the address is invalid.</exception>
    Task SendAsync(EmailMessage email, CancellationToken cancellationToken);
}

/// <summary>Retrying would not help: the server refused this message permanently.</summary>
public sealed class PermanentEmailException(string message, Exception? innerException = null) : Exception(message, innerException);

internal static class SmtpResilience
{
    public const string PipelineName = "smtp";

    /// <summary>
    /// Retry and trip the breaker only for transient failures (4xx, timeouts, connection errors).
    /// A permanent rejection says nothing about the server's health: an invalid recipient must not open the circuit
    /// and stop every other e-mail.
    /// </summary>
    public static bool IsTransient(Exception exception) =>
        exception is not (PermanentEmailException or BrokenCircuitException or OperationCanceledException);
}

/// <summary>Sends through the pooled SMTP connections inside the shared (singleton) resilience pipeline.</summary>
internal sealed class SmtpEmailSender(
    SmtpClientPool pool,
    ResiliencePipelineProvider<string> pipelines,
    IOptions<SmtpOptions> options) : IEmailSender
{
    private readonly ResiliencePipeline _pipeline = pipelines.GetPipeline(SmtpResilience.PipelineName);
    private readonly SmtpOptions _options = options.Value;

    public async Task SendAsync(EmailMessage email, CancellationToken cancellationToken)
    {
        var message = Build(email);

        // Static lambda + state: no closure allocation per send.
        await _pipeline.ExecuteAsync(
            static async (state, token) =>
            {
                try
                {
                    await state.Pool.SendAsync(state.Message, token);
                }
                catch (SmtpCommandException ex) when ((int)ex.StatusCode >= 500)
                {
                    throw new PermanentEmailException($"SMTP {(int)ex.StatusCode}: {ex.Message}", ex);
                }
            },
            (Pool: pool, Message: message),
            cancellationToken);
    }

    private MimeMessage Build(EmailMessage email)
    {
        if (!MailboxAddress.TryParse(email.To, out var recipient))
        {
            throw new PermanentEmailException($"Invalid recipient address '{email.To}'.");
        }

        var message = new MimeMessage { MessageId = email.MessageId, Subject = email.Subject };
        message.From.Add(new MailboxAddress(_options.FromName, _options.FromAddress));
        message.To.Add(recipient);
        message.Body = new BodyBuilder { TextBody = email.TextBody, HtmlBody = email.HtmlBody }.ToMessageBody();
        return message;
    }
}
