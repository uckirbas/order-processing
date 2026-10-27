# Order Processing

Yoğun trafikli bir e-ticaret platformu için sipariş yönetimi ve asenkron işleme (e-posta, fatura): .NET 10, ASP.NET Core, EF Core, PostgreSQL, RabbitMQ.

Mimari, kararlar ve trade-off'lar için: **[DESIGN.md](DESIGN.md)**

> **Durum:** Uçtan uca akış ve consumer micro-batching tamam (Aşama 5). Sırada nginx + replica'larla yük testi var.

## Kapsam

Odak, case'te istenen yoğun trafik ve dayanıklılık: idempotency, transactional outbox, retry/DLQ, circuit breaker, bağlantı ve eşzamanlılık sınırları. Bu bir case study olduğu için, işin özüne katkısı olmayan bazı konular bilinçli olarak sade tutuldu:

- **Ortam ayrımı ve secret yönetimi:** Tek bir lokal ortam var. Bağlantı bilgileri ve şifreler `appsettings.json` ve `docker-compose.yml` içinde düz metin; hepsi yalnızca lokal geliştirme içindir. Prod'da bu değerler ortam değişkenlerinden ya da bir secret store'dan (Vault, AWS Secrets Manager, Azure Key Vault, Kubernetes Secret) gelir. .NET'in configuration katmanı bunu kod değişikliği olmadan destekler: örneğin `ConnectionStrings__Ordering` ortam değişkeni `appsettings.json`'daki değeri ezer.
- **Dış sağlayıcılar:** E-posta gerçek SMTP ile gönderilir ama hedef, maili dışarı iletmeyen Mailpit'tir. Fatura sağlayıcısı simüle edilir: doğrulama ve kalem hesabı gerçek, sağlayıcı çağrısı gecikme ve isteğe bağlı hata enjeksiyonuyla taklit edilir; PDF/XML üretilmez. Gerçek entegrasyon için `IInvoiceGenerator`'ın yeni bir implementasyonu yeterli.
- **Auth, TLS, gözlemlenebilirlik altyapısı** kapsam dışı. Prod'da gerekenler [DESIGN.md §15–16](DESIGN.md#15-bilinen-sınırlar)'da.

## Gereksinimler

- .NET 10 SDK
- Docker (Docker Desktop: en az 8 CPU ve 8 GB bellek önerilir)

## Çalıştırma

```bash
# 1. Altyapı (PostgreSQL, PgBouncer, RabbitMQ, Mailpit)
docker compose up -d --wait

# 2. Migration'lar (doğrudan PostgreSQL'e, PgBouncer'a değil)
dotnet tool restore
dotnet ef database update --project src/Ordering/Ordering.Infrastructure --startup-project src/Ordering/Ordering.Infrastructure
dotnet ef database update --project src/Notification/Notification.Worker --startup-project src/Notification/Notification.Worker
dotnet ef database update --project src/Invoicing/Invoicing.Worker --startup-project src/Invoicing/Invoicing.Worker

# 3. Ordering API → Swagger UI: http://localhost:5080/swagger
dotnet run --project src/Ordering/Ordering.Api --urls http://localhost:5080

# 4. Ordering Worker (outbox relay → RabbitMQ). Birden fazla kopya aynı anda çalışabilir.
dotnet run --project src/Ordering/Ordering.Worker

# 5. Notification Worker → gönderilen e-postalar: http://localhost:8025 (Mailpit)
dotnet run --project src/Notification/Notification.Worker

# 6. Invoicing Worker (fatura üretimi + kendi outbox relay'i)
dotnet run --project src/Invoicing/Invoicing.Worker
```

Servisler herhangi bir sırayla başlatılabilir. Mesajlar kuyruklarda ve outbox'larda bekler, servis açılınca işlenir.

### Akışı uçtan uca görmek

1. Bir sipariş verin (yukarıdaki `curl`). Mailpit'te iki mail görünür: "Siparişiniz alındı" ve "Faturanız hazır".
2. `GET /orders/{id}` siparişin durumunu önce `Received`, fatura üretilince `Invoiced` olarak gösterir.
3. **Kalıcı hata ve DLQ replay:** `"currency":"GBP"` ile sipariş verin. Bu para birimi faturalamada desteklenmiyor:
   - Fatura `Failed` olur, sipariş `InvoiceFailed` olur, mesaj `invoicing.order-placed.dlq`'ya düşer; yalnızca sipariş maili gider.
   - Invoicing'i GBP desteğiyle yeniden başlatın: `InvoiceGenerator__SupportedCurrencies__3=GBP dotnet run --project src/Invoicing/Invoicing.Worker`
   - Management UI → Queues → `invoicing.order-placed.dlq` → **Move messages** → hedef: `invoicing.order-placed`.
   - Aynı fatura `Generated` olur, sipariş `Invoiced` olur, fatura maili gider.

SMTP hatalarını denemek için Mailpit'in chaos özelliği açık. Örneğin alıcıların %30'una geçici hata (451) döndürmek için:

```bash
curl -X PUT http://localhost:8025/api/v1/chaos -H 'Content-Type: application/json' \
  -d '{"Recipient":{"ErrorCode":451,"Probability":30}}'
# 550 → kalıcı hata (mail Dead olur ve mesaj DLQ'ya gider); Probability 0 → kapatır
```

Consumer'lar varsayılan olarak batch çalışır (`Consumers:<Event>:MaxBatchSize`, appsettings'te 20–100). Karşılaştırma için mesaj başına moda ortam değişkeniyle geçilebilir, örneğin `Consumers__OrderPlaced__MaxBatchSize=1`. Bu modda `Concurrency` aynı anda işlenen mesaj sayısı olduğu için onu da artırmak gerekir (örneğin `Consumers__OrderPlaced__Concurrency=16`). Ölçüm sonuçları [DESIGN.md §12](DESIGN.md#12-performans-ve-kapasite)'de.

Worker ilk açılışta RabbitMQ topolojisinin tamamını (exchange'ler, kuyruklar, retry seviyeleri, DLQ'lar) kurar. Yayınlanan mesajlar Management UI'da `invoicing.order-placed` ve `notification.order-placed` kuyruklarında görülebilir.

Örnek istek:

```bash
curl -i http://localhost:5080/orders \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"customerId":"6a1e4c52-8f0e-4b8e-9f3e-1c2d3e4f5a6b","customerEmail":"ali@example.com","currency":"TRY",
       "items":[{"productId":"SKU-1","productName":"Kablosuz Kulaklık","quantity":2,"unitPrice":1499.90}]}'
```

Aynı `Idempotency-Key` ile tekrar gönderilen istek yeni sipariş oluşturmaz; ilk siparişi `200` ve `Idempotent-Replayed: true` ile döndürür.

## Testler

```bash
dotnet test   # Docker gerekir: testler Testcontainers ile gerçek bir PostgreSQL açar
```

| Bileşen | Adres | Kullanıcı / şifre (yalnızca lokal) |
|---|---|---|
| RabbitMQ Management UI | http://localhost:15672 | `orderprocessing` / `orderprocessing` |
| Mailpit (test e-posta arayüzü) | http://localhost:8025 | — |
| PgBouncer (uygulamalar buraya bağlanır) | `localhost:16432` | servis kullanıcıları, aşağıda |
| PostgreSQL (migration ve debug için) | `localhost:15432` | `postgres` / `postgres` |

Servis başına database ve kullanıcı: `ordering` / `ordering_svc`, `invoicing` / `invoicing_svc`, `notification` / `notification_svc`. Şifreler `<kullanıcı>_pwd` biçiminde.

Host portları başka bir lokal kurulumla çakışırsa `POSTGRES_HOST_PORT` ve `PGBOUNCER_HOST_PORT` ortam değişkenleriyle değiştirilebilir.

Her şeyi verisiyle birlikte silmek için: `docker compose down -v`
