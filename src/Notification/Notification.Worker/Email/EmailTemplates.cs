using System.Globalization;
using System.Net;
using System.Text;
using Contracts;
using Notification.Worker.Persistence;

namespace Notification.Worker.Email;

/// <summary>
/// Turkish e-mail bodies built from the event payload alone (fat events: no lookup in another service).
/// Product names come from the client, so they are HTML-encoded before they go into the HTML part.
/// </summary>
internal static class EmailTemplates
{
    private static readonly CultureInfo Turkish = CultureInfo.GetCultureInfo("tr-TR");

    public static EmailMessage OrderReceived(OrderPlaced order)
    {
        var text = new StringBuilder()
            .AppendLine("Merhaba,").AppendLine()
            .AppendLine($"{order.OrderReference} referanslı siparişinizi aldık.").AppendLine();
        foreach (var item in order.Items)
        {
            text.AppendLine($"- {item.ProductName}: {item.Quantity} adet x {Money(item.UnitPrice, order.Currency)} = {Money(item.Quantity * item.UnitPrice, order.Currency)}");
        }

        text.AppendLine().AppendLine($"Toplam: {Money(order.TotalAmount, order.Currency)}").AppendLine()
            .AppendLine("Faturanız hazır olduğunda ayrıca bilgilendirileceksiniz.");

        var rows = new StringBuilder();
        foreach (var item in order.Items)
        {
            rows.Append("<tr>")
                .Append("<td>").Append(Html(item.ProductName)).Append("</td>")
                .Append("<td style=\"text-align:right\">").Append(item.Quantity).Append("</td>")
                .Append("<td style=\"text-align:right\">").Append(Money(item.UnitPrice, order.Currency)).Append("</td>")
                .Append("<td style=\"text-align:right\">").Append(Money(item.Quantity * item.UnitPrice, order.Currency)).Append("</td>")
                .Append("</tr>");
        }

        var html = $"""
            <p>Merhaba,</p>
            <p><strong>{Html(order.OrderReference)}</strong> referanslı siparişinizi aldık.</p>
            <table cellpadding="6" style="border-collapse:collapse">
              <thead><tr><th align="left">Ürün</th><th>Adet</th><th>Birim fiyat</th><th>Tutar</th></tr></thead>
              <tbody>{rows}</tbody>
              <tfoot><tr><td colspan="3" align="right"><strong>Toplam</strong></td><td align="right"><strong>{Money(order.TotalAmount, order.Currency)}</strong></td></tr></tfoot>
            </table>
            <p>Faturanız hazır olduğunda ayrıca bilgilendirileceksiniz.</p>
            """;

        return new EmailMessage(
            MessageId(order.OrderId, NotificationType.OrderReceived),
            order.CustomerEmail,
            $"Siparişiniz alındı ({order.OrderReference})",
            text.ToString(),
            html);
    }

    public static EmailMessage InvoiceReady(InvoiceGenerated invoice)
    {
        var total = Money(invoice.TotalAmount, invoice.Currency);
        var text = $"""
            Merhaba,

            {invoice.OrderReference} referanslı siparişinizin faturası hazırlandı.

            Fatura no: {invoice.InvoiceNumber}
            Tutar: {total}
            """;
        var html = $"""
            <p>Merhaba,</p>
            <p><strong>{Html(invoice.OrderReference)}</strong> referanslı siparişinizin faturası hazırlandı.</p>
            <p>Fatura no: <strong>{Html(invoice.InvoiceNumber)}</strong><br/>Tutar: <strong>{total}</strong></p>
            """;

        return new EmailMessage(
            MessageId(invoice.OrderId, NotificationType.InvoiceReady),
            invoice.CustomerEmail,
            $"Faturanız hazır ({invoice.InvoiceNumber})",
            text,
            html);
    }

    /// <summary>Explicit culture: the host's culture (often invariant in containers) must not change how money looks.</summary>
    private static string Money(decimal amount, string currency) =>
        $"{amount.ToString("N2", Turkish)} {currency}";

    private static string Html(string value) => WebUtility.HtmlEncode(value);

    private static string MessageId(Guid orderId, NotificationType type) =>
        $"{orderId:N}.{type.ToString().ToLowerInvariant()}@order-processing.local";
}
