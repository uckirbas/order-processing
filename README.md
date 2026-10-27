# Order Processing

Yoğun trafikli bir e-ticaret platformu için sipariş yönetimi ve asenkron işleme (e-posta, fatura): .NET 10, ASP.NET Core, EF Core, PostgreSQL, RabbitMQ.

Mimari, kararlar ve trade-off'lar için: **[DESIGN.md](DESIGN.md)**

> **Durum:** Tamam: uçtan uca akış, consumer micro-batching, container'lı kurulum (nginx + replica'lar) ve k6 yük testleri. Sonuçlar aşağıda, [Yük testi](#yük-testi) bölümünde.

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

1. Bir sipariş verin (aşağıdaki örnek istek). Mailpit'te iki mail görünür: "Siparişiniz alındı" ve "Faturanız hazır".
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

## Adresler

| Bileşen | Adres | Kullanıcı / şifre (yalnızca lokal) |
|---|---|---|
| Ordering API (`dotnet run`) | http://localhost:5080/swagger | — |
| Ordering API (container'lar, nginx arkasında) | http://localhost:8080/swagger | — |
| RabbitMQ Management UI | http://localhost:15672 | `orderprocessing` / `orderprocessing` |
| Mailpit (test e-posta arayüzü) | http://localhost:8025 | — |
| PgBouncer (uygulamalar buraya bağlanır) | `localhost:16432` | servis kullanıcıları, aşağıda |
| PostgreSQL (migration ve debug için) | `localhost:15432` | `postgres` / `postgres` |

Servis başına database ve kullanıcı: `ordering` / `ordering_svc`, `invoicing` / `invoicing_svc`, `notification` / `notification_svc`. Şifreler `<kullanıcı>_pwd` biçiminde.

Host portları başka bir lokal kurulumla çakışırsa `POSTGRES_HOST_PORT`, `PGBOUNCER_HOST_PORT` ve `API_HOST_PORT` ortam değişkenleriyle değiştirilebilir.

## Testler

```bash
dotnet test   # Docker gerekir: testler Testcontainers ile gerçek bir PostgreSQL açar
```

## Yük testi

Servisler container'da, üretime benzer bir düzende çalışır: nginx → 3 API kopyası, her worker'dan 2 kopya. Yük [k6](https://k6.io) ile verilir; Docker image'ı kullanıldığı için kurulum gerekmez.

```bash
# Altyapı ve migration'lar için yukarıdaki 1. ve 2. adımlar, sonra:
docker compose --profile app build
docker compose --profile app up -d                  # API: http://localhost:8080

# 8000 sipariş/sn'ye 60 sn rampa + 60 sn sabit (açık model: API yavaşlasa da k6 hızı düşürmez)
docker compose run --rm k6 run /scripts/orders.js
# Sabit hız
docker compose run --rm -e PROFILE=steady -e RATE=4000 -e DURATION=30s k6 run /scripts/orders.js

# Worker'ların birikimi eritme hızı: batch, sonra mesaj başına
loadtest/drain.sh 60000
loadtest/drain.sh 60000 -f docker-compose.yml -f loadtest/per-message.yml

# Her şey birlikte: 60 sn boyunca 3000 sipariş/sn
loadtest/full.sh 3000 60s
```

Yeni başlamış kopyalara ilk saniyeden tam yük vermeyin; script'ler kısa, rampalı bir ısınmayla başlar (nedeni DESIGN.md'de).

**Sonuçlar** (tek laptop: Apple M4, 10 çekirdek; Docker'a 10 CPU / 8 GB. k6 dahil her şey aynı CPU'yu paylaşıyor, bu yüzden sayılar kapasitenin değil davranışın göstergesi):

| Ölçüm | Sonuç |
|---|---|
| API tek başına | 4000 sipariş/sn'ye kadar hepsi kabul: p95 75 ms, p99 160 ms. Tavan ~4000–4500/sn, sınırlayan makinenin toplam CPU'su |
| Kapasitenin 2 katı (8000/sn) | ~4000/sn kabul ve işlenmeye devam ediyor, fazlası anında `503 + Retry-After`; p99 441 ms, sıfır hata |
| Birikim eritme (486 bin sipariş) | Outbox ~50 sn'de yayınlandı (~10.000 mesaj/sn); fatura ~2850/sn (simüle sağlayıcının limiti), e-posta ~1900/sn (Mailpit'in sınırı) |
| Batch ve mesaj başına | Arka plan commit'i sipariş başına 7,05 → 0,17; PostgreSQL CPU'su sipariş başına ~2,5 ms → ~0,27 ms |
| Her şey birlikte (3000/sn yük) | Makine ~1400–1500/sn'de doyuyor; kabul edilen her şeyin bitmesi batch'te 252 sn, mesaj başına 381 sn |

8000 sipariş/sn hedefine bu laptopta tam sistemle ulaşılamıyor. Ölçülen sipariş başı maliyetlerle 8000/sn için gereken kaynak hesabı (API pod sayısı, PostgreSQL ve PgBouncer çekirdekleri) ve yük testinin ortaya çıkardığı sorunlar [DESIGN.md §12](DESIGN.md#yük-testi-k6)'de.

## Temizlik

Her şeyi verisiyle birlikte silmek için: `docker compose --profile app down -v`
