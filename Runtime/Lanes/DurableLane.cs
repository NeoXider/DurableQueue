using System;
using System.Collections.Generic;
using System.Threading;
using Cysharp.Threading.Tasks;
using Neoxider.DurableQueue.Ports;

namespace Neoxider.DurableQueue.Lanes
{
    /// <summary>
    /// Долговечная полоса: постановка не касается сети, доставка переживает перезапуск.
    ///
    /// <para><b>Когда она, а когда немедленная.</b> Долговечная нужна, если ответ не нужен прямо
    /// сейчас ИЛИ запись не имеет права потеряться. Условия соединены «или», и второе сильнее
    /// первого: запись, которую нельзя потерять, идёт сюда даже тогда, когда ответ хотелось бы
    /// получить сразу. Обратное рассуждение — «мы же ждём ответ, значит немедленная» — и приводит к
    /// потере данных: вызов возвращает результат, результат никто не читает, а факт не доехал.</para>
    ///
    /// <para><b>Цена такого выбора платится вызывающим:</b> он обязан уметь продолжить без ответа
    /// сервера. Результат применяется локально сразу, доставку гарантирует очередь, подтверждение
    /// приходит позже и меняет отображение, а не ход работы.</para>
    ///
    /// <para><b>Не потокобезопасна.</b> Рассчитана на однопоточный цикл приложения; повторный вход
    /// в насос отсекается, но параллельные постановки из разных потоков не поддерживаются.</para>
    /// </summary>
    public sealed class DurableLane<T>
    {
        private readonly IQueueDispatcher<T> _dispatcher;
        private readonly IQueueRetryPolicy _policy;
        private readonly IQueueClock _clock;
        private readonly IQueueLog _log;
        private readonly DurableLaneOptions _options;
        private readonly QueueStore<T> _store;
        private readonly QueueDeadLetter _deadLetter;

        private bool _pumping;
        private readonly HashSet<string> _reportedUnknownKinds = new(StringComparer.Ordinal);
        private DateTime? _suspendedUntilUtc;

        public DurableLane(
            IQueueStorage storage,
            IQueueDispatcher<T> dispatcher,
            IQueueRetryPolicy policy,
            DurableLaneOptions options,
            IQueueCodec<T> codec = null,
            IQueueClock clock = null,
            IQueueLog log = null)
        {
            _dispatcher = dispatcher;
            _policy = policy;
            _options = options;
            _clock = clock ?? new SystemQueueClock();
            _log = log ?? new SilentQueueLog();
            _store = new QueueStore<T>(
                storage,
                codec ?? new NewtonsoftQueueCodec<T>(),
                _log,
                options.RecordsKey,
                options.Limits,
                options.Streams);
            _deadLetter = new QueueDeadLetter(
                storage, _log, options.DeadLetterKey, options.Limits.DeadLetterCap);
            _store.Incident += OnIncident;
        }

        /// <summary>Потери и превышения — единственный способ узнать о них, кроме журнала.</summary>
        public event Action<QueueIncident> Incident;

        public int Count => _store.Count;

        public IReadOnlyList<QueueRecord<T>> Records => _store.Records;

        public bool IsSuspended => _suspendedUntilUtc.HasValue && _clock.UtcNow < _suspendedUntilUtc.Value;

        /// <summary>
        /// Ставит запись. Синхронно и без ожидания: постановка сети не касается, иначе вызывающий
        /// снова оказывается заложником сервера — ровно того, от чего очередь его освобождает.
        /// </summary>
        public QueueEnqueueResult Enqueue(
            string kind,
            string stream,
            T payload,
            QueueRecordClass recordClass = QueueRecordClass.Event,
            string idempotencyKey = null,
            string target = null)
        {
            QueueRecord<T> record = new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Kind = kind,
                Stream = stream,
                Class = recordClass,
                IdempotencyKey = idempotencyKey,
                Target = target,
                Payload = payload,
                CreatedAtUtc = _clock.UtcNow
            };

            return _store.Add(record);
        }

        /// <summary>
        /// Ставит адрес всем записям потока, у которых его нет, и СОХРАНЯЕТ НА НОСИТЕЛЬ ДО ВОЗВРАТА.
        ///
        /// <para>Сохранение до сетевого вызова обязательно, и это не перестраховка: упавшая отправка
        /// иначе вернёт записи в очередь снова безадресными, а следующая смена адреса увезёт их
        /// туда, куда они не писались. Именно этот дефект чинили в исходном коде, из которого
        /// библиотека выросла.</para>
        ///
        /// <para>Адрес вычисляет ПРИЛОЖЕНИЕ и зовёт этот метод само — критерий «чей это адрес»
        /// предметный и внутрь библиотеки не попадает.</para>
        /// </summary>
        public int AssignTargetToUnaddressed(string stream, string target)
        {
            if (string.IsNullOrEmpty(target))
            {
                return 0;
            }

            int assigned = 0;
            IReadOnlyList<QueueRecord<T>> records = _store.Records;
            for (int i = 0; i < records.Count; i++)
            {
                QueueRecord<T> record = records[i];
                if (!string.Equals(record.Stream, stream, StringComparison.Ordinal) || record.HasTarget)
                {
                    continue;
                }

                record.Target = target;
                assigned++;
            }

            if (assigned > 0)
            {
                _store.Save(true);
            }

            return assigned;
        }

        /// <summary>
        /// Переносит записи с адреса, которого больше нет, на новый.
        ///
        /// <para>Зовётся ТОЛЬКО по доказательству от сервера, что прежнего адреса не существует.
        /// «Мы локально забыли адрес» доказательством не является: там прежний адрес жив, и запись
        /// обязана уехать именно в него. Без переноса очередь с зафиксированным адресом уходила бы
        /// на тот же отказ бесконечно.</para>
        /// </summary>
        public int Readdress(string stream, string from, string to)
        {
            if (string.IsNullOrEmpty(to))
            {
                return 0;
            }

            int moved = 0;
            IReadOnlyList<QueueRecord<T>> records = _store.Records;
            for (int i = 0; i < records.Count; i++)
            {
                QueueRecord<T> record = records[i];
                if (!string.Equals(record.Stream, stream, StringComparison.Ordinal)
                    || !string.Equals(record.Target, from, StringComparison.Ordinal))
                {
                    continue;
                }

                record.Target = to;
                record.NextAttemptAtUtc = null;
                moved++;
            }

            if (moved > 0)
            {
                _store.Save(true);
                _log.Warn($"Durable queue re-addressed {moved} record(s) in stream '{stream}': " +
                          "the previous target does not exist on the server any more");
            }

            return moved;
        }

        /// <summary>
        /// Снимает приостановку. Зовёт приложение — из точки, где причина приостановки заведомо
        /// снята. Полагаться на этот вызов библиотека не имеет права: у приостановки есть ещё и
        /// потолок по времени, потому что замолчавшую навсегда очередь снаружи не отличить от пустой.
        /// </summary>
        public void Resume() => _suspendedUntilUtc = null;

        /// <summary>
        /// Один проход насоса: обходит потоки по кругу и отправляет голову каждого.
        ///
        /// <para>Проход последовательный, без параллелизма: у сервера есть ограничение частоты, и
        /// очередь, разгребающая себя залпом после долгого простоя, сама себя и заблокирует.</para>
        ///
        /// <para>Поток остаётся в обходе, пока его записи уезжают; любой другой исход выводит его из
        /// прохода — иначе политика, попросившая повторить немедленно, закрутила бы насос навсегда.</para>
        /// </summary>
        public async UniTask PumpAsync(CancellationToken cancellationToken = default)
        {
            if (_pumping)
            {
                return;
            }

            _store.EnsureLoaded();
            if (!_store.IsLoaded)
            {
                // Ключа ещё нет, сохранённое не прочитано. Отправлять сейчас — значит увезти
                // поставленное вслепую ВПЕРЁД непрочитанной истории и нарушить порядок, который
                // очередь и обязана держать. Задержка здесь дешевле перепутанного порядка.
                return;
            }

            if (IsSuspendedAt(_clock.UtcNow))
            {
                return;
            }

            _pumping = true;
            try
            {
                ExpireRecords(_clock.UtcNow);

                HashSet<string> parked = new(StringComparer.Ordinal);
                HashSet<string> parkedTargets = new(StringComparer.Ordinal);
                bool progressed = true;
                while (progressed && !cancellationToken.IsCancellationRequested)
                {
                    progressed = false;
                    foreach (string stream in CollectStreams())
                    {
                        if (cancellationToken.IsCancellationRequested || parked.Contains(stream))
                        {
                            continue;
                        }

                        DateTime now = _clock.UtcNow;
                        if (IsSuspendedAt(now))
                        {
                            return;
                        }

                        QueueBatch<T> batch = TakeLeadingBatch(stream, now, parkedTargets);
                        if (batch == null)
                        {
                            parked.Add(stream);
                            continue;
                        }

                        QueueResponse response = await DeliverAsync(batch, cancellationToken);
                        if (response == null)
                        {
                            // Отмена вызывающего. Не ошибка доставки: запись остаётся в голове,
                            // счётчики не трогаются, попытка не расходуется.
                            return;
                        }

                        if (ApplyDecision(batch, response, _clock.UtcNow))
                        {
                            progressed = true;
                        }
                        else if (_options.Streams.Get(stream).IndependentTargets)
                        {
                            // Застрял один адрес, а не поток: остальные адреса пробуются в этом же
                            // проходе.
                            parkedTargets.Add(TargetKey(stream, batch.Target));
                            progressed = true;
                        }
                        else
                        {
                            parked.Add(stream);
                        }
                    }
                }
            }
            finally
            {
                _pumping = false;
            }
        }

        public QueueSnapshot Snapshot()
        {
            IReadOnlyList<QueueRecord<T>> records = _store.Records;
            List<QueueStreamSnapshot> streams = new();
            foreach (string stream in CollectStreams())
            {
                int count = 0;
                QueueRecord<T> head = null;
                for (int i = 0; i < records.Count; i++)
                {
                    if (!string.Equals(records[i].Stream, stream, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    count++;
                    head ??= records[i];
                }

                streams.Add(new QueueStreamSnapshot(
                    stream,
                    count,
                    head?.Kind,
                    head is { HasTarget: true },
                    head?.NextAttemptAtUtc,
                    head?.Attempts ?? 0,
                    head?.TransportFailures ?? 0));
            }

            return new QueueSnapshot(
                records.Count,
                _store.MeasureAll(),
                _store.IsLoaded,
                _suspendedUntilUtc,
                streams,
                _deadLetter.Entries);
        }

        /// <summary>
        /// Полная очистка без доставки. Точка обслуживания: продуктовый путь её не зовёт — смена
        /// адреса хвост не уничтожает, а досылает.
        /// </summary>
        public void Clear() => _store.Clear();

        public void ClearDeadLetter() => _deadLetter.Clear();

        private async UniTask<QueueResponse> DeliverAsync(
            QueueBatch<T> batch, CancellationToken cancellationToken)
        {
            try
            {
                return await _dispatcher.DeliverAsync(batch, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return null;
            }
            catch (Exception exception)
            {
                // Превращать свой отказ в ответ обязан диспетчер. Если он этого не сделал, считаем
                // случившееся транспортной ошибкой: потолок попыток она не расходует, потому что
                // сервер ничего не сказал. Молчать при этом нельзя — иначе дефект диспетчера
                // выглядит как «сеть шалит» и не чинится никогда.
                _log.Error($"Durable queue dispatcher threw {exception.GetType().Name} on stream " +
                           $"'{batch.Stream}' (kind={batch.Kind}); treated as a transport failure");
                return QueueResponse.FromException(new QueueEndpoint(string.Empty, batch.Kind), exception);
            }
        }

        /// <summary>
        /// Применяет решение политики. Возвращает <c>true</c>, если поток стоит обойти ещё раз.
        /// </summary>
        private bool ApplyDecision(QueueBatch<T> batch, QueueResponse response, DateTime now)
        {
            batch = SettleRejections(batch, now);
            if (batch == null)
            {
                return true;
            }

            QueueAttempt attempt = new(
                batch.Kind,
                batch.Stream,
                batch.Target,
                batch.Head.Class,
                batch.Head.Attempts,
                batch.Head.TransportFailures,
                batch.Head.CreatedAtUtc,
                response.Endpoint,
                response);

            QueueRetryDecision decision = _policy.Decide(attempt);
            QueueStreamOptions options = _options.Streams.Get(batch.Stream);

            switch (decision.Action)
            {
                case QueueRetryAction.Done:
                    _store.Remove(batch.Records);
                    _store.Save(options.FlushOnWrite);
                    return true;

                case QueueRetryAction.Drop:
                    FailBatch(batch, response, QueueIncidentReason.PermanentRejection, decision.Reason, now);
                    return false;

                case QueueRetryAction.Suspend:
                    // Приостановка целиком, без счётчиков и без потерь: это «сейчас нельзя
                    // отправлять ничего», а не «эта запись плоха».
                    _suspendedUntilUtc = now + _options.Limits.SuspendCeiling;
                    _log.Warn($"Durable queue is suspended until {_suspendedUntilUtc:O} " +
                              $"({decision.Reason ?? "no reason given"}); no attempt was spent");
                    return false;

                default:
                    RescheduleBatch(batch, response, decision, now);
                    return false;
            }
        }

        /// <summary>
        /// Снимает записи с отдельным исходом: подтверждённые — без следа, отвергнутые — с тем же следом, что и у
        /// постоянного отказа всей пачки. Возвращает пачку из оставшихся записей или <c>null</c>, если
        /// не осталось ни одной.
        /// </summary>
        private QueueBatch<T> SettleRejections(QueueBatch<T> batch, DateTime now)
        {
            IReadOnlyList<QueueRejection<T>> rejections = batch.Rejections;
            IReadOnlyList<QueueRecord<T>> confirmed = batch.Confirmed;
            if (rejections.Count == 0 && confirmed.Count == 0)
            {
                return batch;
            }

            List<QueueRecord<T>> remaining = new(batch.Records);
            for (int i = 0; i < confirmed.Count; i++)
            {
                remaining.Remove(confirmed[i]);
                _store.Remove(confirmed[i]);
            }

            for (int i = 0; i < rejections.Count; i++)
            {
                QueueRejection<T> rejection = rejections[i];
                QueueRecord<T> record = rejection.Record;
                _deadLetter.Add(new QueueDeadLetterEntry
                {
                    Kind = record.Kind,
                    Stream = record.Stream,
                    IdempotencyKey = record.IdempotencyKey,
                    Reason = rejection.Reason ?? QueueIncidentReason.PermanentRejection.ToString(),
                    StatusCode = rejection.StatusCode,
                    Error = rejection.Error,
                    Attempts = record.Attempts,
                    TransportFailures = record.TransportFailures,
                    CreatedAtUtc = record.CreatedAtUtc,
                    FailedAtUtc = now
                });
                OnIncident(new QueueIncident(
                    QueueIncidentReason.PermanentRejection,
                    record.Stream,
                    record.Kind,
                    record.IdempotencyKey,
                    rejection.StatusCode,
                    rejection.Reason,
                    createdAtUtc: record.CreatedAtUtc));
                remaining.Remove(record);
                _store.Remove(record);
            }

            _store.Save(true);
            return remaining.Count == 0
                ? null
                : new QueueBatch<T>(batch.Stream, batch.Kind, batch.Target, remaining);
        }

        private void RescheduleBatch(
            QueueBatch<T> batch, QueueResponse response, QueueRetryDecision decision, DateTime now)
        {
            bool serverAnswered = !response.TransportFailure;
            bool spendsAttempt = serverAnswered && decision.SpendsAttempt;
            for (int i = 0; i < batch.Records.Count; i++)
            {
                QueueRecord<T> record = batch.Records[i];
                if (spendsAttempt)
                {
                    record.Attempts++;
                }
                else if (serverAnswered)
                {
                    record.FreeRetries++;
                }
                else
                {
                    record.TransportFailures++;
                }

                record.LastError = string.IsNullOrEmpty(response.Error)
                    ? $"status {response.StatusCode}"
                    : response.Error;
            }

            if (spendsAttempt && batch.Head.Attempts >= _options.Limits.MaxAttempts)
            {
                FailBatch(batch, response, QueueIncidentReason.AttemptsExhausted, decision.Reason, now);
                return;
            }

            TimeSpan delay = decision.Delay ?? _options.Backoff.Next(batch.Head.Failures - 1);
            DateTime nextAttempt = now + delay;
            for (int i = 0; i < batch.Records.Count; i++)
            {
                batch.Records[i].NextAttemptAtUtc = nextAttempt;
            }

            _store.Save(false);
        }

        /// <summary>
        /// Снимает записи, доставить которые не удалось, и оставляет от них СЛЕД: строку в
        /// dead-letter, ошибку в журнале и происшествие для приложения. Без следа это и есть та
        /// молчаливая потеря, ради устранения которой очередь существует.
        /// </summary>
        private void FailBatch(
            QueueBatch<T> batch,
            QueueResponse response,
            QueueIncidentReason reason,
            string detail,
            DateTime now)
        {
            for (int i = 0; i < batch.Records.Count; i++)
            {
                QueueRecord<T> record = batch.Records[i];
                _deadLetter.Add(new QueueDeadLetterEntry
                {
                    Kind = record.Kind,
                    Stream = record.Stream,
                    IdempotencyKey = record.IdempotencyKey,
                    Reason = detail ?? reason.ToString(),
                    StatusCode = response?.StatusCode ?? 0,
                    Error = response?.Error,
                    Attempts = record.Attempts,
                    TransportFailures = record.TransportFailures,
                    CreatedAtUtc = record.CreatedAtUtc,
                    FailedAtUtc = now
                });

                OnIncident(new QueueIncident(
                    reason,
                    record.Stream,
                    record.Kind,
                    record.IdempotencyKey,
                    response?.StatusCode ?? 0,
                    detail,
                    createdAtUtc: record.CreatedAtUtc));
            }

            _store.Remove(batch.Records);
            _store.Save(true);
        }

        /// <summary>
        /// Снимает просроченные записи. Исход зависит от класса, и разница несущая: протухший снимок
        /// ХУЖЕ отсутствующего — доехав, он перезапишет более свежее состояние, поэтому его просто
        /// нет. А протухший факт — это потерянный факт, и уйти он обязан в dead-letter: молча
        /// удалять недоставленное по таймеру — то же самое, от чего очередь и защищает.
        /// </summary>
        private void ExpireRecords(DateTime now)
        {
            IReadOnlyList<QueueRecord<T>> records = _store.Records;
            List<QueueRecord<T>> expired = null;
            for (int i = 0; i < records.Count; i++)
            {
                if (now - records[i].CreatedAtUtc < _options.Limits.Ttl
                    || _options.Streams.Get(records[i].Stream).NeverExpires)
                {
                    continue;
                }

                expired ??= new List<QueueRecord<T>>();
                expired.Add(records[i]);
            }

            if (expired == null)
            {
                return;
            }

            for (int i = 0; i < expired.Count; i++)
            {
                QueueRecord<T> record = expired[i];
                if (record.Class == QueueRecordClass.State)
                {
                    _log.Warn($"Durable queue dropped an expired snapshot: stream={record.Stream} " +
                              $"kind={record.Kind}; a stale snapshot is worse than none");
                    OnIncident(new QueueIncident(
                        QueueIncidentReason.Expired, record.Stream, record.Kind,
                        record.IdempotencyKey, 0, "expired", createdAtUtc: record.CreatedAtUtc));
                }
                else
                {
                    _deadLetter.Add(new QueueDeadLetterEntry
                    {
                        Kind = record.Kind,
                        Stream = record.Stream,
                        IdempotencyKey = record.IdempotencyKey,
                        Reason = "expired",
                        StatusCode = 0,
                        Error = record.LastError,
                        Attempts = record.Attempts,
                        TransportFailures = record.TransportFailures,
                        CreatedAtUtc = record.CreatedAtUtc,
                        FailedAtUtc = now
                    });
                    OnIncident(new QueueIncident(
                        QueueIncidentReason.Expired, record.Stream, record.Kind,
                        record.IdempotencyKey, 0, "expired", createdAtUtc: record.CreatedAtUtc));
                }

                _store.Remove(record);
            }

            _store.Save(true);
        }

        private List<string> CollectStreams()
        {
            IReadOnlyList<QueueRecord<T>> records = _store.Records;
            List<string> streams = new();
            HashSet<string> seen = new(StringComparer.Ordinal);
            for (int i = 0; i < records.Count; i++)
            {
                string stream = records[i].Stream ?? string.Empty;
                if (seen.Add(stream))
                {
                    streams.Add(stream);
                }
            }

            return streams;
        }

        /// <summary>
        /// Ведущий отрезок потока: подряд идущие записи одного вида с одним адресом.
        ///
        /// <para><c>null</c> — потоку сейчас нечего или нельзя отправлять. Записи без адреса насос не
        /// отправляет и попыткой это не считает: слать некуда, а расходовать на «адреса ещё нет»
        /// попытку — превращать нормальное состояние в отказ.</para>
        /// </summary>
        private QueueBatch<T> TakeLeadingBatch(string stream, DateTime now, HashSet<string> parkedTargets)
        {
            IReadOnlyList<QueueRecord<T>> records = _store.Records;
            QueueRecord<T> head = null;
            List<QueueRecord<T>> batch = null;
            QueueStreamOptions streamOptions = _options.Streams.Get(stream);
            int maxBatchSize = streamOptions.MaxBatchSize;
            bool independent = streamOptions.IndependentTargets;
            HashSet<string> seenTargets = independent ? new HashSet<string>(StringComparer.Ordinal) : null;

            for (int i = 0; i < records.Count; i++)
            {
                QueueRecord<T> record = records[i];
                if (!string.Equals(record.Stream ?? string.Empty, stream, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!_options.IsKnownKind(record.Kind))
                {
                    ReportUnknownKind(record);
                    continue;
                }

                if (head == null)
                {
                    if (independent)
                    {
                        string target = record.Target ?? string.Empty;
                        if (!seenTargets.Add(target)
                            || !record.HasTarget
                            || !record.IsDue(now)
                            || parkedTargets.Contains(TargetKey(stream, target)))
                        {
                            // Не голова своего адреса, адреса нет, рано или адрес уже не принял
                            // запись в этом проходе — пробуем следующий адрес.
                            continue;
                        }
                    }
                    else if (!record.HasTarget || !record.IsDue(now))
                    {
                        return null;
                    }

                    head = record;
                    batch = new List<QueueRecord<T>> { record };

                    // Снимок в пачку не собирается: глубина потока снимков всегда 1, а объединять
                    // разные снимки в один запрос значит отправить устаревший.
                    if (record.Class == QueueRecordClass.State)
                    {
                        break;
                    }

                    continue;
                }

                if (independent && !string.Equals(record.Target, head.Target, StringComparison.Ordinal))
                {
                    continue;
                }

                if (record.Class != QueueRecordClass.Event
                    || !string.Equals(record.Kind, head.Kind, StringComparison.Ordinal)
                    || !string.Equals(record.Target, head.Target, StringComparison.Ordinal)
                    || (maxBatchSize > 0 && batch.Count >= maxBatchSize))
                {
                    break;
                }

                batch.Add(record);
            }

            return head == null ? null : new QueueBatch<T>(stream, head.Kind, head.Target, batch);
        }

        private static string TargetKey(string stream, string target) => stream + "\n" + (target ?? string.Empty);

        private void ReportUnknownKind(QueueRecord<T> record)
        {
            if (_reportedUnknownKinds.Add(record.Kind ?? string.Empty))
            {
                _log.Warn($"Durable queue keeps records of unknown kind '{record.Kind}' in stream " +
                          $"'{record.Stream}' without sending them; a version that knows the kind will");
            }
        }

        private bool IsSuspendedAt(DateTime now)
        {
            if (!_suspendedUntilUtc.HasValue)
            {
                return false;
            }

            if (now < _suspendedUntilUtc.Value)
            {
                return true;
            }

            // Потолок по времени истёк — делаем пробную отправку. Страховка от забытого
            // возобновления: приостановку, которую некому снять, снаружи не отличить от пустой
            // очереди, и обнаруживается она пропажей данных.
            _suspendedUntilUtc = null;
            return false;
        }

        private void OnIncident(QueueIncident incident) => Incident?.Invoke(incident);

        /// <summary>
        /// Журнал по умолчанию — молчаливый. Умолчание выбрано так намеренно: библиотека не имеет
        /// права писать в чужую консоль, если её об этом не просили.
        /// </summary>
        private sealed class SilentQueueLog : IQueueLog
        {
            public void Warn(string message)
            {
            }

            public void Error(string message)
            {
            }
        }
    }
}
