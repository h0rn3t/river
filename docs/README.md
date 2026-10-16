# River [![Build Status](https://github.com/h0rn3t/river/actions/workflows/ci.yaml/badge.svg?branch=master&event=push)](https://github.com/h0rn3t/river/actions/workflows/ci.yaml?query=branch%3Amaster+event%3Apush) [![codecov](https://codecov.io/gh/h0rn3t/river/branch/master/graph/badge.svg)](https://codecov.io/gh/h0rn3t/river) [![Go Reference](https://pkg.go.dev/badge/github.com/h0rn3t/river.svg)](https://pkg.go.dev/github.com/h0rn3t/river)

River — швидка транзакційна черга задач для Go, яка зберігає задачі в Postgres,
SQLite або Microsoft SQL Server. Вона автоматично повторює задачі після помилок,
планує їх на майбутнє, підтримує унікальні задачі та має веб-інтерфейс.

Цей репозиторій — форк [riverqueue/river][upstream] з модулем
`github.com/h0rn3t/river`. Головна відмінність від upstream — драйвер для
[Microsoft SQL Server](#microsoft-sql-server). [Документація][docs] та
[домашня сторінка][homepage] описують upstream-версію англійською мовою: у
прикладах звідти замініть `github.com/riverqueue/river` на
`github.com/h0rn3t/river`. Довідник API — на [pkg.go.dev][godoc].

River радить тримати дані застосунку та чергу задач в одній базі даних. Якщо
додавати задачі в тій самій транзакції, що й інші зміни в базі, зникає цілий
клас проблем розподілених систем. Задача гарантовано потрапляє в чергу, якщо
транзакцію закомічено, зникає, якщо транзакцію відкочено, і не стає доступною
для виконання _до_ коміту. Докладніше про цей підхід — у статті
[transactional enqueueing].

## Встановлення

Потрібен Go 1.26 або новіший. Встановіть River і драйвер для своєї бази даних
(див. [підтримувані бази даних](#підтримувані-бази-даних)):

```shell
go get github.com/h0rn3t/river github.com/h0rn3t/river/riverdriver/riverpgxv5
```

Перед першим запуском створіть таблиці River міграціями. Найпростіше зробити це
через CLI:

```shell
go install github.com/h0rn3t/river/cmd/river@latest
river migrate-up --line main --database-url "postgres:///river_dev"
```

CLI приймає URL зі схемами `postgres://`, `postgresql://`, `sqlite://` та
`sqlserver://`. Міграції також можна запускати з коду через пакет
[`rivermigrate`].

## Підтримувані бази даних

| База даних | Драйвер | Пул з'єднань |
| --- | --- | --- |
| Postgres | [`riverpgxv5`] | `*pgxpool.Pool` з [pgx v5] |
| Postgres | [`riverdatabasesql`] | `*sql.DB` (`database/sql`, зокрема разом із Bun чи GORM) |
| SQLite, libSQL, Turso | [`riversqlite`] | `*sql.DB` |
| Microsoft SQL Server 2022+, Azure SQL Database | [`riversqlserver`] | `*sql.DB` з [go-mssqldb] |

YugabyteDB працює через драйвери Postgres, див. [YugabyteDB][yugabyte].

Драйвери SQLite та SQL Server перебувають на ранньому етапі тестування: їх
перевіряє набір тестів River, але в реальних системах вони поки що майже не
використовувалися.

Приклади нижче використовують `riverpgxv5`. Для інших баз змінюється лише
драйвер, який передається в `river.NewClient`.

## Аргументи задач і воркери

У Go задача описується парою структур: реалізацією [`JobArgs`] та реалізацією
[`Worker`].

Аргументи задачі мають теги `json`, які визначають, як задача серіалізується в
базу даних і десеріалізується з неї. Також вони задають «kind» — стабільний
рядок, який однозначно ідентифікує тип задачі.

```go
type SortArgs struct {
    // Strings — зріз рядків, які треба відсортувати.
    Strings []string `json:"strings"`
}

func (SortArgs) Kind() string { return "sort" }
```

Воркер має метод `Work`, який визначає, як виконується задача.

```go
type SortWorker struct {
    // Вбудований WorkerDefaults додає типові реалізації решти методів
    // інтерфейсу Worker:
    river.WorkerDefaults[SortArgs]
}

func (w *SortWorker) Work(ctx context.Context, job *river.Job[SortArgs]) error {
    sort.Strings(job.Args.Strings)
    fmt.Printf("Sorted strings: %+v\n", job.Args.Strings)
    return nil
}
```

## Реєстрація воркерів

Задачі однозначно ідентифікуються рядком «kind». Воркери реєструються під час
запуску, щоб River знав, якому воркеру віддати задачу:

```go
workers := river.NewWorkers()
// AddWorker панікує, якщо воркер уже зареєстрований або некоректний:
river.AddWorker(workers, &SortWorker{})
```

## Запуск клієнта

[`Client`] додає задачі в чергу, керує їх обробкою та
[сервісами обслуговування][maintenance services]. Клієнт створюється з пулом
з'єднань, [драйвером][driver] і структурою конфігурації, де є набір `Workers`
та інші налаштування. Ось клієнт, який обробляє одну чергу (`"default"`) щонайбільше
100 горутинами-воркерами одночасно:

```go
riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
    Queues: map[string]river.QueueConfig{
        river.QueueDefault: {MaxWorkers: 100},
    },
    Workers: workers,
})
if err != nil {
    panic(err)
}

// Запускаємо клієнт. Усі виконувані задачі успадковують ctx:
if err := riverClient.Start(ctx); err != nil {
    panic(err)
}
```

`Workers` можна не вказувати, але краще вказати: тоді River перевірить, що для
кожного kind, який додається в чергу, є воркер.

### Зупинка

Під час завершення програми клієнт треба зупинити. Є кілька способів (див.
[graceful shutdown]), найкоротший — скасувати контекст, переданий у `Start`,
коли програма готова зупинитися. Наприклад, щоб зупинятися за
`SIGINT`/`SIGTERM`:

```go
riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
    SoftStopTimeout: 10 * time.Second,
    ...
})
if err != nil {
    panic(err)
}

signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
defer stop()

// Перестаємо брати нові задачі й чекаємо на завершення активних. Після
// SoftStopTimeout активні задачі скасовуються.
if err := riverClient.Start(signalCtx); err != nil {
    panic(err)
}

<-riverClient.Stopped()
```

Інший варіант — явно викликати `Stop`:

```go
if err := riverClient.Stop(ctx); err != nil {
    panic(err)
}
```

[Клієнти лише для вставки](https://riverqueue.com/docs/insert-only-clients)
додають задачі, але не виконують їх, тому їх не треба запускати чи зупиняти.

## Додавання задач

[`Client.InsertTx`] додає задачу в чергу в межах транзакції:

```go
_, err = riverClient.InsertTx(ctx, tx, SortArgs{
    Strings: []string{
        "whale", "tiger", "bear",
    },
}, nil)

if err != nil {
    panic(err)
}
```

Тип `tx` залежить від драйвера: `pgx.Tx` для `riverpgxv5` і `*sql.Tx` для
`riverdatabasesql`, `riversqlite` та `riversqlserver`.

Повний код — у [прикладі `InsertAndWork`][`InsertAndWork` example].

## Microsoft SQL Server

Драйвер [`riversqlserver`] працює з Microsoft SQL Server 2022 або новішим та з
Azure SQL Database. Він приймає пул `database/sql`, відкритий драйвером
`sqlserver` з бібліотеки [go-mssqldb]:

```shell
go get github.com/h0rn3t/river/riverdriver/riversqlserver github.com/microsoft/go-mssqldb
```

```go
import (
    "database/sql"

    _ "github.com/microsoft/go-mssqldb"

    "github.com/h0rn3t/river"
    "github.com/h0rn3t/river/riverdriver/riversqlserver"
)

dbPool, err := sql.Open("sqlserver", "sqlserver://user:pass@localhost:1433?database=river")
if err != nil {
    panic(err)
}
defer dbPool.Close()

riverClient, err := river.NewClient(riversqlserver.New(dbPool), &river.Config{
    Queues: map[string]river.QueueConfig{
        river.QueueDefault: {MaxWorkers: 100},
    },
    Workers: workers,
})
if err != nil {
    panic(err)
}
```

У `sql.Open` треба вказувати саме ім'я драйвера `sqlserver`, а не `mssql`:
River передає параметри запитів у вигляді `@p1`, `@p2`, …, а такий формат
приймає лише драйвер `sqlserver`.

Таблиці River створюються міграціями так само, як і для інших баз даних:

```shell
river migrate-up --line main --database-url "sqlserver://user:pass@localhost:1433?database=river"
```

Щоб читачі й записувачі менше блокували одне одного, рекомендуємо увімкнути
`READ_COMMITTED_SNAPSHOT` для бази даних:

```sql
ALTER DATABASE river SET READ_COMMITTED_SNAPSHOT ON;
```

Відмінності від Postgres:

  - У SQL Server немає аналога `LISTEN`/`NOTIFY`, тому клієнт працює лише в
    режимі опитування: нові задачі підхоплюються раз на
    `Config.FetchPollInterval` (типово раз на секунду), а не одразу після
    вставки.

  - `JobListParams.Metadata` не підтримується, а SQL у `JobListParams.Where`
    треба писати на T-SQL.

  - Таблиці River використовують фільтрований індекс та індекс на обчислюваному
    стовпці, тому сесії мають працювати з опціями `SET`, яких для цього вимагає
    SQL Server (зокрема з увімкненими `QUOTED_IDENTIFIER` і `ANSI_NULLS`). У
    go-mssqldb вони увімкнені за замовчуванням.

  - Веб-інтерфейс [River UI][riverui] — окремий upstream-проєкт, який SQL Server
    не підтримує.

## Інші можливості

  - [Пакетне додавання задач][Batch job insertion]: багато задач за один запит
    (у драйвері `riverpgxv5` — через `COPY FROM`).

  - [Скасування задач][Cancelling jobs] зсередини функції `Work`.

  - [Обробка помилок і панік][Error and panic handling].

  - [Кілька черг][Multiple queues], щоб гарантувати пропускну здатність,
    доступність воркерів та ізоляцію між компонентами.

  - [Періодичні та cron-задачі][Periodic and cron jobs].

  - [Заплановані задачі][Scheduled jobs], які автоматично запускаються в
    заданий час у майбутньому.

  - [Відкладення задач][Snoozing jobs] зсередини функції `Work`.

  - [Підписки][Subscriptions] на активність черг і статистику, через які зручно
    підключати логування й метрики.

  - [Тестові хелпери][Test helpers], щоб перевіряти, що задачі додаються як
    очікується.

  - [Транзакційне завершення задач][Transactional job completion], щоб
    завершення задачі комітилося разом з іншими змінами в транзакції.

  - [Унікальні задачі][Unique jobs] за аргументами, періодом, чергою та станом.

  - [Веб-інтерфейс][Web UI] для перегляду задач і черг та роботи з ними.

  - [Робочі функції][Work functions] для спрощеної реалізації воркерів.

## Розробка

Див. [розробку River][developing River] (англійською мовою). Тести драйвера
SQL Server запускаються, лише коли задано `TEST_SQLSERVER_DATABASE_URL`; там
само описано, як підняти SQL Server у Docker.

CI збирає покриття тестами з Postgres, SQLite і SQL Server і завантажує його в
[Codecov][codecov]. Локально покриття збирає `make test COVERAGE_DIR=coverage`.

[`Client`]: https://pkg.go.dev/github.com/h0rn3t/river#Client
[`Client.InsertTx`]: https://pkg.go.dev/github.com/h0rn3t/river#Client.InsertTx
[`InsertAndWork` example]: https://pkg.go.dev/github.com/h0rn3t/river#example-package-InsertAndWork
[`JobArgs`]: https://pkg.go.dev/github.com/h0rn3t/river#JobArgs
[`Worker`]: https://pkg.go.dev/github.com/h0rn3t/river#Worker
[`riverdatabasesql`]: https://pkg.go.dev/github.com/h0rn3t/river/riverdriver/riverdatabasesql
[`rivermigrate`]: https://pkg.go.dev/github.com/h0rn3t/river/rivermigrate
[`riverpgxv5`]: https://pkg.go.dev/github.com/h0rn3t/river/riverdriver/riverpgxv5
[`riversqlite`]: https://pkg.go.dev/github.com/h0rn3t/river/riverdriver/riversqlite
[`riversqlserver`]: https://pkg.go.dev/github.com/h0rn3t/river/riverdriver/riversqlserver
[Batch job insertion]: https://riverqueue.com/docs/batch-job-insertion
[Cancelling jobs]: https://riverqueue.com/docs/cancelling-jobs
[Error and panic handling]: https://riverqueue.com/docs/error-handling
[Multiple queues]: https://riverqueue.com/docs/multiple-queues
[Periodic and cron jobs]: https://riverqueue.com/docs/periodic-jobs
[Scheduled jobs]: https://riverqueue.com/docs/scheduled-jobs
[Snoozing jobs]: https://riverqueue.com/docs/snoozing-jobs
[Subscriptions]: https://riverqueue.com/docs/subscriptions
[Test helpers]: https://riverqueue.com/docs/testing
[Transactional job completion]: https://riverqueue.com/docs/transactional-job-completion
[Unique jobs]: https://riverqueue.com/docs/unique-jobs
[Web UI]: https://github.com/riverqueue/riverui
[Work functions]: https://riverqueue.com/docs/work-functions
[codecov]: https://codecov.io/gh/h0rn3t/river
[developing River]: https://github.com/h0rn3t/river/blob/master/docs/development.md
[docs]: https://riverqueue.com/docs
[driver]: https://riverqueue.com/docs/database-drivers
[go-mssqldb]: https://github.com/microsoft/go-mssqldb
[godoc]: https://pkg.go.dev/github.com/h0rn3t/river
[graceful shutdown]: https://riverqueue.com/docs/graceful-shutdown
[homepage]: https://riverqueue.com
[maintenance services]: https://riverqueue.com/docs/maintenance-services
[pgx v5]: https://github.com/jackc/pgx
[riverui]: https://github.com/riverqueue/riverui
[transactional enqueueing]: https://riverqueue.com/docs/transactional-enqueueing
[upstream]: https://github.com/riverqueue/river
[yugabyte]: https://github.com/h0rn3t/river/blob/master/docs/yugabyte.md
