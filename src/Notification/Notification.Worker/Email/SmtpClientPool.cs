using System.Threading.Channels;
using MailKit.Net.Smtp;
using MailKit.Security;
using Microsoft.Extensions.Options;
using MimeKit;

namespace Notification.Worker.Email;

/// <summary>
/// Reuses SMTP connections across sends. MailKit's <see cref="SmtpClient"/> is not thread-safe (one command at a time),
/// so each client is used by exactly one send at a time: it is taken out of the pool, used, and put back.
/// A client that failed mid-conversation is discarded rather than reused in an unknown protocol state.
/// </summary>
internal sealed class SmtpClientPool : IAsyncDisposable
{
    private readonly SmtpOptions _options;
    private readonly SemaphoreSlim _slots;
    private readonly Channel<SmtpClient> _idle = Channel.CreateUnbounded<SmtpClient>();

    public SmtpClientPool(IOptions<SmtpOptions> options)
    {
        _options = options.Value;
        _slots = new SemaphoreSlim(_options.MaxConnections, _options.MaxConnections);
    }

    public async Task SendAsync(MimeMessage message, CancellationToken cancellationToken)
    {
        await _slots.WaitAsync(cancellationToken);
        var client = _idle.Reader.TryRead(out var pooled) ? pooled : new SmtpClient();
        try
        {
            if (!client.IsConnected)
            {
                client.Timeout = (int)_options.AttemptTimeout.TotalMilliseconds;
                await client.ConnectAsync(_options.Host, _options.Port, SecureSocketOptions.None, cancellationToken);
                if (!string.IsNullOrEmpty(_options.UserName))
                {
                    await client.AuthenticateAsync(_options.UserName, _options.Password ?? string.Empty, cancellationToken);
                }
            }

            await client.SendAsync(message, cancellationToken);
            _idle.Writer.TryWrite(client);
            client = null;
        }
        finally
        {
            client?.Dispose();
            _slots.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _idle.Writer.TryComplete();
        while (_idle.Reader.TryRead(out var client))
        {
            if (client.IsConnected)
            {
                await client.DisconnectAsync(quit: true);
            }

            client.Dispose();
        }

        _slots.Dispose();
    }
}
