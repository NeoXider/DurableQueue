using System;
using System.Collections.Generic;
using Neoxider.DurableQueue.Ports;
using Newtonsoft.Json;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Строка dead-letter: МЕТАДАННЫЕ недоставленной записи, без полезной нагрузки.
    ///
    /// <para>Без нагрузки намеренно и с двух сторон. Приватность: нагрузка может содержать что
    /// угодно, а dead-letter живёт дольше самой записи. Объём: копия нагрузки удваивает занятое
    /// место ровно тогда, когда с местом уже плохо.</para>
    /// </summary>
    public sealed class QueueDeadLetterEntry
    {
        [JsonProperty("kind")] public string Kind { get; set; }

        [JsonProperty("stream")] public string Stream { get; set; }

        [JsonProperty("idempotency_key")] public string IdempotencyKey { get; set; }

        [JsonProperty("reason")] public string Reason { get; set; }

        [JsonProperty("status_code")] public int StatusCode { get; set; }

        [JsonProperty("error")] public string Error { get; set; }

        [JsonProperty("attempts")] public int Attempts { get; set; }

        [JsonProperty("transport_failures")] public int TransportFailures { get; set; }

        [JsonProperty("created_at")] public DateTime CreatedAtUtc { get; set; }

        [JsonProperty("failed_at")] public DateTime FailedAtUtc { get; set; }
    }

    /// <summary>
    /// Кольцевой журнал недоставленного с капом. Отдельный ключ хранилища, потому что переживать он
    /// обязан в том числе очистку самой очереди: «куда делась запись» — вопрос, который задают уже
    /// после того, как записи не стало.
    /// </summary>
    public sealed class QueueDeadLetter
    {
        private readonly IQueueStorage _storage;
        private readonly IQueueLog _log;
        private readonly string _key;
        private readonly int _cap;

        private List<QueueDeadLetterEntry> _entries;

        public QueueDeadLetter(IQueueStorage storage, IQueueLog log, string key, int cap)
        {
            _storage = storage;
            _log = log;
            _key = key;
            _cap = Math.Max(1, cap);
        }

        public IReadOnlyList<QueueDeadLetterEntry> Entries
        {
            get
            {
                EnsureLoaded();
                // Пока ключ неадресуем, читать нечего — но отдавать наружу null нельзя: смотреть
                // сюда будут как раз при разборе происшествия, а падение диагностики отнимает
                // последний способ понять, что случилось.
                return _entries ?? (IReadOnlyList<QueueDeadLetterEntry>)Array.Empty<QueueDeadLetterEntry>();
            }
        }

        public void Add(QueueDeadLetterEntry entry)
        {
            EnsureLoaded();
            if (_entries == null)
            {
                // Ключ ещё неадресуем: писать некуда, но и молчать нельзя — журнал остаётся
                // единственным следом.
                _log.Error(BuildLogLine(entry) + " (dead-letter не сохранён: ключ ещё неадресуем)");
                return;
            }

            _entries.Add(entry);
            int dropped = _entries.Count - _cap;
            if (dropped > 0)
            {
                // Журнал dead-letter ограничен ради квоты хранилища; самые старые записи уходят не
                // молча: число потерянных видно в логе.
                _entries.RemoveRange(0, dropped);
                _log.Warn($"Durable queue dead-letter is full (cap {_cap}): dropped {dropped} oldest record(s).");
            }

            Save();
            _log.Error(BuildLogLine(entry));
        }

        public void Clear()
        {
            _entries = new List<QueueDeadLetterEntry>();
            _storage.Delete(_key);
        }

        private static string BuildLogLine(QueueDeadLetterEntry entry)
        {
            return $"Durable queue dead-letter: stream={entry.Stream} kind={entry.Kind} " +
                   $"reason={entry.Reason} status={entry.StatusCode} attempts={entry.Attempts} " +
                   $"transport_failures={entry.TransportFailures} key={entry.IdempotencyKey}";
        }

        private void EnsureLoaded()
        {
            if (_entries != null || !_storage.IsAddressable(_key))
            {
                return;
            }

            _entries = new List<QueueDeadLetterEntry>();
            if (!_storage.TryRead(_key, out string raw) || string.IsNullOrWhiteSpace(raw))
            {
                return;
            }

            try
            {
                List<QueueDeadLetterEntry> stored =
                    JsonConvert.DeserializeObject<List<QueueDeadLetterEntry>>(raw);
                if (stored != null)
                {
                    stored.RemoveAll(entry => entry == null);
                    _entries = stored;
                }
            }
            catch (JsonException exception)
            {
                // Битый журнал недоставленного — не повод потерять очередь: чистим и продолжаем,
                // но говорим об этом вслух.
                _log.Warn($"Durable queue dead-letter is unreadable and was reset: {exception.GetType().Name}");
                _storage.Delete(_key);
            }
        }

        private void Save()
        {
            try
            {
                _storage.Write(_key, JsonConvert.SerializeObject(_entries));
            }
            catch (Exception exception)
            {
                _log.Warn($"Durable queue dead-letter could not be stored: {exception.GetType().Name}");
            }
        }
    }
}
