using System.Collections.Generic;
using Neoxider.DurableQueue.Ports;
using Newtonsoft.Json;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Кодек по умолчанию: конверт целиком, полезная нагрузка вложенным объектом, имена полей в
    /// snake_case.
    ///
    /// <para>Он именно «по умолчанию», а не «единственный»: у приложения, которое переводит на
    /// библиотеку УЖЕ РАБОТАЮЩУЮ очередь, формат лежит на устройствах пользователей, и подменить
    /// кодек дешевле, чем мигрировать чужие данные.</para>
    /// </summary>
    public sealed class NewtonsoftQueueCodec<T> : IQueueCodec<T>
    {
        private readonly JsonSerializerSettings _settings;

        public NewtonsoftQueueCodec(JsonSerializerSettings settings = null)
        {
            _settings = settings ?? new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };
        }

        public string Encode(IReadOnlyList<QueueRecord<T>> records)
        {
            List<StoredRecord> stored = new(records.Count);
            for (int i = 0; i < records.Count; i++)
            {
                stored.Add(StoredRecord.From(records[i]));
            }

            return JsonConvert.SerializeObject(stored, _settings);
        }

        public bool TryDecode(string raw, out List<QueueRecord<T>> records)
        {
            records = new List<QueueRecord<T>>();
            if (string.IsNullOrWhiteSpace(raw))
            {
                return true;
            }

            List<StoredRecord> stored;
            try
            {
                stored = JsonConvert.DeserializeObject<List<StoredRecord>>(raw, _settings);
            }
            catch (JsonException)
            {
                return false;
            }

            if (stored == null)
            {
                return false;
            }

            for (int i = 0; i < stored.Count; i++)
            {
                // Пустых элементов в нашем формате нет, но чужое или битое содержимое не должно
                // ронять отправку разыменованием null уже после старта.
                if (stored[i] != null)
                {
                    records.Add(stored[i].ToRecord());
                }
            }

            return true;
        }

        private sealed class StoredRecord
        {
            [JsonProperty("id")] public string Id { get; set; }

            [JsonProperty("kind")] public string Kind { get; set; }

            [JsonProperty("stream")] public string Stream { get; set; }

            [JsonProperty("class")] public QueueRecordClass Class { get; set; }

            [JsonProperty("idempotency_key")] public string IdempotencyKey { get; set; }

            [JsonProperty("target")] public string Target { get; set; }

            [JsonProperty("payload")] public T Payload { get; set; }

            [JsonProperty("created_at")] public System.DateTime CreatedAtUtc { get; set; }

            [JsonProperty("attempts")] public int Attempts { get; set; }

            [JsonProperty("transport_failures")] public int TransportFailures { get; set; }

            [JsonProperty("free_retries")] public int FreeRetries { get; set; }

            [JsonProperty("next_attempt_at")] public System.DateTime? NextAttemptAtUtc { get; set; }

            [JsonProperty("last_error")] public string LastError { get; set; }

            public static StoredRecord From(QueueRecord<T> record)
            {
                return new StoredRecord
                {
                    Id = record.Id,
                    Kind = record.Kind,
                    Stream = record.Stream,
                    Class = record.Class,
                    IdempotencyKey = record.IdempotencyKey,
                    Target = record.Target,
                    Payload = record.Payload,
                    CreatedAtUtc = record.CreatedAtUtc,
                    Attempts = record.Attempts,
                    TransportFailures = record.TransportFailures,
                    FreeRetries = record.FreeRetries,
                    NextAttemptAtUtc = record.NextAttemptAtUtc,
                    LastError = record.LastError
                };
            }

            public QueueRecord<T> ToRecord()
            {
                return new QueueRecord<T>
                {
                    Id = Id,
                    Kind = Kind,
                    Stream = Stream,
                    Class = Class,
                    IdempotencyKey = IdempotencyKey,
                    Target = Target,
                    Payload = Payload,
                    CreatedAtUtc = CreatedAtUtc,
                    Attempts = Attempts,
                    TransportFailures = TransportFailures,
                    FreeRetries = FreeRetries,
                    NextAttemptAtUtc = NextAttemptAtUtc,
                    LastError = LastError
                };
            }
        }
    }
}
