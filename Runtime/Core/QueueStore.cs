using System;
using System.Collections.Generic;
using System.Text;
using Neoxider.DurableQueue.Ports;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Записи очереди и их жизнь на носителе: отложенная загрузка, сохранение, схлопывание снимков,
    /// бюджет по байтам и вытеснение.
    ///
    /// <para><b>Загрузка отложенная, и это не оптимизация.</b> Ключ часто скоупится по владельцу
    /// данных, а очередь создаётся раньше, чем владелец известен. Прочитать в конструкторе — значит
    /// получить честное «данных нет» и первой же записью затереть всё сохранённое. Поэтому пока
    /// ключ неадресуем, очередь НИЧЕГО не считает прочитанным, держит поставленное в памяти и
    /// дочитывает старое ровно в тот момент, когда ключ появился.</para>
    /// </summary>
    public sealed class QueueStore<T>
    {
        private readonly IQueueStorage _storage;
        private readonly IQueueCodec<T> _codec;
        private readonly IQueueLog _log;
        private readonly QueueLimits _limits;
        private readonly QueueStreamRegistry _streams;
        private readonly string _key;

        private readonly List<QueueRecord<T>> _records = new();

        private bool _loaded;

        public QueueStore(
            IQueueStorage storage,
            IQueueCodec<T> codec,
            IQueueLog log,
            string key,
            QueueLimits limits,
            QueueStreamRegistry streams)
        {
            _storage = storage;
            _codec = codec;
            _log = log;
            _key = key;
            _limits = limits;
            _streams = streams;
        }

        /// <summary>Происшествия — потери и превышения. Подписывается приложение.</summary>
        public event Action<QueueIncident> Incident;

        /// <summary>Прочитано ли сохранённое. Пока <c>false</c>, записи живут только в памяти.</summary>
        public bool IsLoaded => _loaded;

        public IReadOnlyList<QueueRecord<T>> Records
        {
            get
            {
                EnsureLoaded();
                return _records;
            }
        }

        public int Count
        {
            get
            {
                EnsureLoaded();
                return _records.Count;
            }
        }

        /// <summary>
        /// Дочитывает сохранённое, как только ключ стал адресуем. Поставленное вслепую не теряется:
        /// сохранённое встаёт ПЕРЕД ним — так восстанавливается порядок «что раньше записали, то
        /// раньше и уедет».
        /// </summary>
        public void EnsureLoaded()
        {
            if (_loaded || !_storage.IsAddressable(_key))
            {
                return;
            }

            _loaded = true;

            List<QueueRecord<T>> stored = ReadStored();
            if (stored.Count > 0)
            {
                _records.InsertRange(0, stored);
            }

            // Сохранение сразу после слияния: иначе поставленное вслепую живёт только в памяти и
            // пропадёт при закрытии, хотя ключ уже есть.
            Save(false);
        }

        public QueueEnqueueResult Add(QueueRecord<T> record)
        {
            EnsureLoaded();

            int recordBytes = MeasureRecord(record);
            if (recordBytes > _limits.MaxRecordBytes)
            {
                string reason = $"record is {recordBytes} bytes, cap is {_limits.MaxRecordBytes}";
                Report(new QueueIncident(
                    QueueIncidentReason.TooLarge, record.Stream, record.Kind, record.IdempotencyKey,
                    0, reason, bytes: recordBytes));
                _log.Error($"Durable queue rejected a record: stream={record.Stream} " +
                           $"kind={record.Kind} {reason}");
                return QueueEnqueueResult.TooLarge(reason);
            }

            if (record.Class == QueueRecordClass.State)
            {
                Collapse(record);
            }
            else
            {
                _records.Add(record);
            }

            bool overBudget = EnforceBudget();

            QueueStreamOptions options = _streams.Get(record.Stream);
            if (!Save(options.FlushOnWrite))
            {
                if (options.RetainWhenStorageRefuses)
                {
                    // Поток предпочёл доставку без гарантии перезапуска потере: запись остаётся в
                    // памяти, следующее сохранение снова попробует носитель.
                    _log.Error($"Durable queue keeps a record in memory only: stream={record.Stream} " +
                               $"kind={record.Kind} key={record.IdempotencyKey}; it will not survive a restart " +
                               "until the storage accepts it");
                    return QueueEnqueueResult.InMemoryOnly("storage refused the write");
                }

                // Хранилище отказало физически. Запись из памяти убираем: держать её означало бы
                // обещать доставку, которой не будет, и молчать об этом.
                _records.Remove(record);
                return QueueEnqueueResult.StorageFailure("storage refused the write");
            }

            return QueueEnqueueResult.Ok(overBudget);
        }

        public void Remove(IReadOnlyList<QueueRecord<T>> records)
        {
            for (int i = 0; i < records.Count; i++)
            {
                _records.Remove(records[i]);
            }
        }

        public void Remove(QueueRecord<T> record) => _records.Remove(record);

        public void Clear()
        {
            _records.Clear();
            _storage.Delete(_key);
            _storage.Flush();
        }

        /// <summary>
        /// Пишет очередь на носитель. <c>false</c> — хранилище отказало; исключение наружу не
        /// выходит никогда, потому что вызывающий этой ветки не ждёт и обработать её не может.
        /// </summary>
        public bool Save(bool flush)
        {
            if (!_loaded)
            {
                // Ключа ещё нет: писать некуда, но и терять нечего — записи лежат в памяти и
                // сохранятся, как только ключ появится.
                return true;
            }

            try
            {
                if (!_storage.Write(_key, _codec.Encode(_records)))
                {
                    ReportStorageRefusal("storage returned false");
                    return false;
                }
            }
            catch (Exception exception)
            {
                // Реализация обязана возвращать false, а не бросать. Ловим на случай, если она это
                // правило нарушила: отказ носителя не должен превращаться в исключение в кадре
                // обычной работы приложения.
                ReportStorageRefusal(exception.GetType().Name);
                return false;
            }

            if (flush)
            {
                _storage.Flush();
            }

            return true;
        }

        public int MeasureAll() => Utf8Bytes(_codec.Encode(_records));

        private int MeasureRecord(QueueRecord<T> record) =>
            Utf8Bytes(_codec.Encode(new[] { record }));

        private static int Utf8Bytes(string value) =>
            string.IsNullOrEmpty(value) ? 0 : Encoding.UTF8.GetByteCount(value);

        /// <summary>
        /// Снимок вытесняет предыдущий снимок того же вида в том же потоке, ЗАНИМАЯ ЕГО МЕСТО в
        /// очереди: глубина потока снимков всегда 1.
        ///
        /// <para>Конверт переписывается целиком, а не «обновляется значение»: у нового содержимого
        /// свой ключ идемпотентности и свой возраст. Унаследовать ключ вытесненной записи значило бы
        /// однажды получить от сервера «уже применено» на данные, которых он не видел.</para>
        /// </summary>
        private void Collapse(QueueRecord<T> record)
        {
            for (int i = 0; i < _records.Count; i++)
            {
                QueueRecord<T> existing = _records[i];
                if (existing.Class != QueueRecordClass.State
                    || !string.Equals(existing.Stream, record.Stream, StringComparison.Ordinal)
                    || !string.Equals(existing.Kind, record.Kind, StringComparison.Ordinal))
                {
                    continue;
                }

                _records[i] = record;
                return;
            }

            _records.Add(record);
        }

        /// <summary>
        /// Держит мягкий бюджет. Возвращает <c>true</c>, если удержать не удалось и бюджет превышен
        /// осознанно.
        ///
        /// <para>Порядок жертв назначает приложение; здесь только исполнение: сначала потоки с
        /// меньшим приоритетом, внутри потока — самые старые. Неприкосновенные потоки не трогаются
        /// НИКОГДА, даже если места нет совсем: превышение бюджета — наша внутренняя проблема,
        /// потеря чьей-то работы — уже не наша.</para>
        /// </summary>
        private bool EnforceBudget()
        {
            int total = MeasureAll();
            if (total <= _limits.TotalBudgetBytes)
            {
                return false;
            }

            Dictionary<string, int> evictedCount = new();
            Dictionary<string, int> evictedBytes = new();

            while (total > _limits.TotalBudgetBytes)
            {
                int victim = FindEvictionVictim();
                if (victim < 0)
                {
                    break;
                }

                QueueRecord<T> record = _records[victim];
                int bytes = MeasureRecord(record);
                _records.RemoveAt(victim);
                total -= bytes;

                evictedCount.TryGetValue(record.Stream, out int count);
                evictedCount[record.Stream] = count + 1;
                evictedBytes.TryGetValue(record.Stream, out int size);
                evictedBytes[record.Stream] = size + bytes;
            }

            foreach (KeyValuePair<string, int> pair in evictedCount)
            {
                QueueStreamOptions options = _streams.Get(pair.Key);
                int bytes = evictedBytes[pair.Key];
                string message = $"Durable queue evicted {pair.Value} record(s) from stream " +
                                 $"'{pair.Key}' ({bytes} bytes) to stay inside the budget";
                if (options.EvictionSeverity == QueueEvictionSeverity.Error)
                {
                    _log.Error(message);
                }
                else
                {
                    _log.Warn(message);
                }

                Report(new QueueIncident(
                    QueueIncidentReason.Evicted, pair.Key, null, null, 0, message,
                    pair.Value, bytes));
            }

            if (total <= _limits.TotalBudgetBytes)
            {
                return false;
            }

            string overBudget = $"Durable queue is over its budget: {total} bytes against " +
                                $"{_limits.TotalBudgetBytes}; nothing else may be dropped";
            _log.Error(overBudget);
            Report(new QueueIncident(
                QueueIncidentReason.OverBudget, null, null, null, 0, overBudget, bytes: total));
            return true;
        }

        private int FindEvictionVictim()
        {
            int victim = -1;
            int victimPriority = int.MaxValue;

            for (int i = 0; i < _records.Count; i++)
            {
                QueueStreamOptions options = _streams.Get(_records[i].Stream);
                if (!options.Evictable || options.EvictionPriority >= victimPriority)
                {
                    continue;
                }

                victim = i;
                victimPriority = options.EvictionPriority;
            }

            return victim;
        }

        private List<QueueRecord<T>> ReadStored()
        {
            if (!_storage.TryRead(_key, out string raw) || string.IsNullOrWhiteSpace(raw))
            {
                return new List<QueueRecord<T>>();
            }

            if (_codec.TryDecode(raw, out List<QueueRecord<T>> stored) && stored != null)
            {
                return stored;
            }

            // Битое содержимое удаляем и говорим об этом. Молча вернуть пустой список — значит
            // потерять данные так, что этого не видно ни в журнале, ни в диагностике, а битое
            // значение продолжает лежать в хранилище и мешать следующим запускам.
            _log.Error($"Durable queue content at '{_key}' is unreadable and was removed");
            _storage.Delete(_key);
            return new List<QueueRecord<T>>();
        }

        private void ReportStorageRefusal(string detail)
        {
            _log.Error($"Durable queue could not be stored at '{_key}': {detail}");
            Report(new QueueIncident(
                QueueIncidentReason.StorageRefused, null, null, null, 0, detail));
        }

        private void Report(QueueIncident incident) => Incident?.Invoke(incident);
    }
}
