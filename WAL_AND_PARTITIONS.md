# Режимы `PartitionedTable` и `WalMessages`: как это работает

Документ разделён на две части:
1. **Простое объяснение (без кода и SQL)** — 3 лёгкие схемы, чтобы за минуту понять всю идею целиком.
2. **Техническая часть (под капотом)** — точные компоненты C#, таблицы PostgreSQL, защитные проверки, быстрое переключение и метрики.

---

# Часть 1. Как это работает (простыми словами)

## 1.1. Как сообщение идёт в обычном режиме (`INSERT` в почасовую таблицу)

Главное отличие от классического варианта: Приложение 1 **только вставляет (`INSERT`)** и никогда не делает `DELETE` — поэтому на таблице нет мёртвых строк и нагрузки от `autovacuum`.

```mermaid
flowchart LR
    App["Приложение 1\n(бизнес-транзакция)"]
    Part["Таблица текущего часа\n(уже создана заранее)"]
    WAL[("Журнал WAL\nв PostgreSQL")]
    Svc["Сервис доставки\n(Active-лидер)"]
    Kafka[("Kafka\nтопики + позиция")]

    App -->|"1. INSERT (без DELETE)"| Part
    Part -->|"2. Сразу попадает в WAL"| WAL
    WAL -->|"3. Читаем поток"| Svc
    Svc -->|"4. Пишем в Kafka\nи сохраняем позицию"| Kafka
    Svc -.->|"5. Сообщаем PG:\nдосюда всё доставлено"| WAL
```

> [!TIP]
> Если переключить флаг `p_write_to_table = false`, шаг с таблицей пропускается и сообщение летит **напрямую в журнал WAL** (целевой режим `WalMessages`). Слот пересоздавать не нужно.

---

## 1.2. Что делает фоновая джоба раз в час (в `HH:05`)

Джоба работает **только на активном лидере**. Первый раз — сразу при старте, дальше — раз в час ровно в `HH:05` (например `13:05`, `14:05`, `15:05`).

```mermaid
flowchart TD
    Wake(["Проснулась в HH:05"])
    Create["1. Проверка запаса на 2 дня вперёд\nНе хватает будущих таблиц?\nДосоздаём недостающие"]
    Seal["2. Закрытие прошлого часа\nЧас 12:00-13:00 уже закончился 5 мин назад:\nфиксируем текущую позицию WAL как границу часа"]
    Check{"3. Kafka уже перешла\nграницу старого часа\nи нет аварии?"}
    Drop["4. Удаляем старую таблицу\n(целиком, мгновенно, без DELETE)"]
    Keep["Ничего не удаляем\n(ждём следующего часа)"]
    Sleep(["Спим ровно до следующего HH:05"])

    Wake --> Create
    Create --> Seal
    Seal --> Check
    Check -->|"Да"| Drop
    Check -->|"Нет"| Keep
    Drop --> Sleep
    Keep --> Sleep
```

**Как это выглядит по часам на примере таблицы `12:00–13:00`:**
- **За 48 часов до `12:00`** — таблица уже создана пустой про запас (если джоба вдруг сломается, есть 2 дня на реакцию).
- **`12:00–13:00`** — Приложение 1 спокойно делает в неё `INSERT`.
- **`13:05`** — джоба фиксирует границу WAL для часа `12:00–13:00` (5 минут запаса нужны, чтобы транзакции, начатые в `12:59`, успели закоммититься). При этом таблица `11:00–12:00` уже удаляется.
- **`14:05`** — джоба видит, что Kafka давно прошла границу `13:05`, и **удаляет таблицу `12:00–13:00`**. Если Kafka стояла 3 часа, таблица просто ждёт на диске; когда Kafka догонит — джоба снесёт все доставленные часы за один раз.

---

## 1.3. Что происходит, если слот WAL умер (авто-восстановление 24/7)

Если слот удалили, переполнился WAL или произошёл сбой кластера — **люди не нужны**, сервис чинит себя сам и ничего не теряет благодаря сохранённым часовым таблицам.

```mermaid
flowchart TD
    Boom(["Авария: слот пропал\nили сломался"])
    Lock["1. Ставим замок на уборщика\nНи одна таблица не может удалиться!"]
    Ensure["2. Проверяем запас таблиц на 2 дня\nПриложение 1 продолжает писать без сбоев"]
    NewSlot["3. Создаём новый слот\n(с текущей точки WAL)"]
    Replay["4. Дочитываем пропущенное\nиз часовых таблиц в Kafka"]
    Unlock["5. Сохраняем новую позицию\nи снимаем замок с уборщика"]
    Ok(["Продолжаем обычную\nдоставку из нового слота"])

    Boom --> Lock
    Lock --> Ensure
    Ensure --> NewSlot
    NewSlot --> Replay
    Replay --> Unlock
    Unlock --> Ok
```

**Почему при этом ничего не теряется:**
1. Замок гарантирует, что уборщик не удалит старые таблицы, пока мы не вычитаем «дыру».
2. Сначала создаётся **новый слот** (точка `A`), и **сразу после** делается чтение из часовых таблиц (снимок `B >= A`). Всё, что было до `A`, попадает в снимок; всё, что после `A`, поймает новый слот.

---

# Часть 2. Техническая схема (под капотом)

Ниже — точная карта для разработчиков, ревью и эксплуатации: какие классы, таблицы БД, SQL-функции и проверки стоят за каждым шагом.

## 2.1. Компоненты и переключатель режимов

```mermaid
flowchart LR
    subgraph Writer["Источник (Приложение 1 / Writer)"]
        PubFn["outbox.publish_message\n(p_write_to_table)"]
    end

    subgraph PG["PostgreSQL 17 Primary"]
        TPart["outbox.messages_partitioned\n(RANGE по created_at)"]
        WalMsg["pg_logical_emit_message\n(формат OBX1)"]
        Slot[("failover slot\npgoutput v1")]
        Meta[("partition_registry\npartition_guard")]
    end

    subgraph Core["CdcOutbox.Core (Active-лидер)"]
        Reader["OutboxSlotReader\n(Insert + LogicalDecoding\n+ FlushSlotOnIdle)"]
        Pub["OutboxPublisher\n(Kafka TX + CheckpointStore)"]
        Worker["OutboxPartitionMaintenanceWorker\n(старт + HH:05 UTC)"]
        Recovery["PartitionRecoveryService\n(24/7 Replay)"]
    end

    subgraph Kafka["Apache Kafka"]
        Out[("Бизнес-топики")]
        Chk[("cdc-checkpoints")]
    end

    PubFn -->|"true"| TPart
    PubFn -->|"false"| WalMsg
    TPart -->|"publish_via_partition_root=true"| Slot
    WalMsg -->|"messages=true"| Slot

    Slot --> Reader
    Reader -->|"Bounded Channel"| Pub
    Pub --> Out
    Pub --> Chk
    Pub -->|"checkpoint LSN"| Reader
    Reader -->|"confirmed_flush_lsn"| Slot

    Worker <--> Meta
    Worker -->|"CREATE / DROP"| TPart
    Recovery <--> Meta
    Recovery -->|"REPEATABLE READ"| TPart
    Recovery -->|"Drop + Create failover slot"| Slot
    Recovery --> Pub
```

| Режим `CdcOutbox:Storage:Mode` | Publication | Хранение в БД | Фоновая джоба партиций | Авто-восстановление 24/7 |
|---|---|---|---|---|
| `Table` *(по умолчанию)* | `outbox_publication` | `outbox.messages` (`INSERT+DELETE`) | Выключена | Штатный ручной `RecoveryMode` |
| `WalMessages` | `outbox_wal_publication` (пустая) | Нет таблицы (только WAL `OBX1`) | Выключена | Штатный ручной `RecoveryMode` |
| `PartitionedTable` | `outbox_partitioned_publication` (`publish_via_partition_root=true`) | `outbox.messages_partitioned` (`INSERT`-only) | Включена автоматически на Active | Включено автоматически (`AutoRecoverFromPartitions=true`) |

---

## 2.2. Сквозная последовательность доставки и простоев (`OutboxSlotReader` → `OutboxPublisher`)

```mermaid
sequenceDiagram
    autonumber
    participant App as Приложение 1
    participant PG as PostgreSQL 17
    participant Reader as OutboxSlotReader
    participant Pub as OutboxPublisher
    participant Kafka as Kafka

    App->>PG: BEGIN + бизнес-логика + INSERT в messages_partitioned + COMMIT (commit_lsn)
    PG-->>Reader: Begin -> Insert (от корня) -> Commit(commit_lsn)
    Reader->>Pub: Запись пачки в Bounded Channel (MarkQueuedMessageLsn)

    Pub->>Kafka: BeginTransaction -> Produce батч + Produce checkpoint(commit_lsn) -> CommitTransaction
    Kafka-->>Pub: ACK (acks=all)

    Pub-->>Reader: MarkDelivered + MarkCheckpointed(commit_lsn)
    Reader->>PG: SendStatusUpdate(confirmed_flush_lsn = commit_lsn)

    Note over PG,Reader: В простое (FlushSlotOnIdle): после полного тика тишины без смены эпохи потока
    PG-->>Reader: Пустой Commit или Keepalive (LastReceivedLsn)
    Reader-->>Pub: MarkIdleLsn -> Checkpoint TX -> SendStatusUpdate(confirmed_flush_lsn)
```

---

## 2.3. Внутренние шаги джобы `OutboxPartitionMaintenanceWorker`

```mermaid
flowchart TD
    Start(["Старт Active-лидера"])
    Run1["Немедленный RunOnceSafelyAsync"]
    Sleep["Сон до HH:05:00 UTC\n(CalculateNextRunUtc, без дрейфа)"]
    S1["1. EnsurePartitionsAsync\nТекущий час + 48ч вперёд"]
    S2["2. SealClosedPartitionsAsync\nsealed_lsn = pg_current_wal_lsn()\nдля часов старше 5 мин"]
    S3["3. UpdateDeliveryProgressAsync\nЗапись checkpoint в partition_guard"]
    S4{"4. Проверка partition_guard\nrecovery_in_progress=false?\nsystem_identifier совпал?\nsealed_lsn <= min(chk, guard)?"}
    Drop["DropDeliveredPartitionsAsync\nSECURITY DEFINER (lock_timeout 2s)\nили прямой DETACH CONCURRENTLY"]
    Skip["Пропуск удаления\n(партиции не трогаем)"]

    Start --> Run1
    Run1 --> Sleep
    Sleep -->|"HH:05 UTC"| S1
    S1 --> S2
    S2 --> S3
    S3 --> S4
    S4 -->|"Да"| Drop
    S4 -->|"Нет"| Skip
    Drop --> Sleep
    Skip --> Sleep
```

| Шаг | Таблица / SQL-функция (`infrastructure/postgres/init/04-wal-and-partitions.sql`) | Защита / метрика |
|---|---|---|
| 1. Предсоздание 48ч | `outbox.ensure_partitions(...)` → `outbox.partition_registry` | `outbox_future_partitions_count` (WARN если `< 48`, алерт `< 24`), `outbox_partitions_created_total` |
| 2. Запечатывание | `outbox.seal_closed_partitions(...)` → `partition_registry.sealed_lsn` | `range_end_utc <= now() - SealGraceMinutes(5)` |
| 3. Защитный курсор | `outbox.update_partition_guard_progress(...)` → `outbox.partition_guard` | Монотонный рост только при `recovery_in_progress = false` и совпадении `system_identifier` |
| 4. Удаление | `outbox.drop_delivered_partitions(...)` (или прямой `DETACH CONCURRENTLY`) | `SET LOCAL lock_timeout = '2s'`, пропуск при `lock_not_available`, `outbox_partitions_dropped_total`; сбой джобы не роняет живую доставку |

---

## 2.4. Внутренний поток 24/7 авто-восстановления (`LeadershipCoordinator` + `PartitionRecoveryService`)

```mermaid
flowchart TD
    LiveErr["Слот умер в рантайме\nSQLSTATE 55000 (invalidated)"]
    Restart["OutboxProcessor -> SlotRecoveryRequiredException\nLeadershipCoordinator: StopTerm + новый IProducer"]
    Startup["OutboxStartupChecks\nRunPartitionedStartupAsync"]
    Check{"Обнаружен разрыв?\n(lost / удалён / скачок LSN /\nсмена sysId / recovery=true)"}
    RunRec["PartitionRecoveryService.RecoverAsync:\n1) recovery_in_progress = true\n2) EnsurePartitions (48ч)\n3) Drop + Create failover slot (LSN_A)\n4) REPEATABLE READ SELECT (LSN_B >= LSN_A)\n5) OutboxPublisher.PublishRecoveryBatchAsync\n6) checkpoint = LSN_A\n7) recovery_in_progress = false"]
    PubCheck["PublicationVerifier.VerifyAsync"]
    Live(["Запуск живого стрима\nOutboxProcessor.RunAsync"])

    LiveErr --> Restart
    Restart --> Startup
    Startup --> Check
    Check -->|"Нет"| PubCheck
    Check -->|"Да"| RunRec
    RunRec --> PubCheck
    PubCheck --> Live
```

| Событие / деталь | Где реализовано |
|---|---|
| Обнаружение смерти слота в рантайме | `OutboxProcessor.HandleReadingFailure` → `SlotRecoveryRequiredException` |
| Безопасная смена эпохи продюсера (без гонки с `IProducer`) | `LeadershipCoordinator.SuperviseDeliveryAsync` (остановка срока → `Dispose` старого `IProducer` → `GrantAsync` со свежим `IProducer`) |
| Определение причины разрыва при старте | `OutboxStartupChecks.DetectPartitionRecoveryReasonAsync` |
| Пошаговый replay без потерь | `PartitionRecoveryService.RecoverAsync` (`RecoveryOverlapMinutes = 5`, `RecoveryBatchSize = 1000`, метрики `outbox_partition_auto_recoveries_total` и `outbox_partition_recovered_messages_total`) |

---

## 2.5. Быстрое переключение режимов на стенде

```bash
# 1. Штатный режим (по умолчанию — без изменений)
docker compose up -d

# 2. Страхующие почасовые партиции (INSERT-only, 48ч запас, очистка в HH:05, 24/7 авто-восстановление)
CDCOUTBOX__STORAGE__MODE=PartitionedTable \
CDCOUTBOX__STORAGE__TABLE=messages_partitioned \
CDCOUTBOX__STORAGE__ENABLEWALMESSAGES=true \
CDCOUTBOX__REPLICATION__PUBLICATIONNAME=outbox_partitioned_publication \
WRITER__DEFAULTS__WRITEMODE=InsertOnly \
WRITER__DEFAULTS__TABLENAME=outbox.messages_partitioned \
docker compose up -d

# 3. Целевой безтабличный WAL (pg_logical_emit_message, формат OBX1, пустая publication)
CDCOUTBOX__STORAGE__MODE=WalMessages \
CDCOUTBOX__REPLICATION__PUBLICATIONNAME=outbox_wal_publication \
WRITER__DEFAULTS__WRITEMODE=WalMessage \
docker compose up -d
```

Все параметры конфигурации описаны в [CONFIGURATION.md](CONFIGURATION.md), а метрики — в [METRICS.md](METRICS.md).
