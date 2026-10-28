using System;
using System.Collections.Generic;
using System.Threading;
using Cysharp.Threading.Tasks;
using Neoxider.DurableQueue.Lanes;
using Neoxider.DurableQueue.Ports;

namespace Neoxider.DurableQueue.Tests
{
    /// <summary>
    /// Хранилище в памяти с двумя переключателями, ради которых оно и существует: ключ может быть
    /// ещё неадресуем, а запись может отказать. Оба состояния — реальные, оба однажды стоили данных.
    /// </summary>
    public sealed class FakeQueueStorage : IQueueStorage
    {
        private readonly Dictionary<string, string> _values = new();

        public bool Addressable { get; set; } = true;

        /// <summary>Хранилище отвечает «не записал» — вежливый отказ.</summary>
        public bool WriteRefuses { get; set; }

        /// <summary>Хранилище бросает исключение — невежливый отказ, наружу он выйти не должен.</summary>
        public bool WriteThrows { get; set; }

        public int FlushCount { get; private set; }

        public bool IsAddressable(string key) => Addressable;

        public bool TryRead(string key, out string value)
        {
            value = null;
            return Addressable && _values.TryGetValue(key, out value);
        }

        public bool Write(string key, string value)
        {
            if (WriteThrows)
            {
                throw new InvalidOperationException("storage is full");
            }

            if (WriteRefuses)
            {
                return false;
            }

            _values[key] = value;
            return true;
        }

        public void Delete(string key) => _values.Remove(key);

        public void Flush() => FlushCount++;

        public string Raw(string key) => _values.TryGetValue(key, out string value) ? value : null;

        public void Seed(string key, string value) => _values[key] = value;
    }

    public sealed class FakeQueueClock : IQueueClock
    {
        public DateTime UtcNow { get; set; } = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        public void Advance(TimeSpan delta) => UtcNow += delta;
    }

    public sealed class FakeQueueLog : IQueueLog
    {
        public List<string> Warnings { get; } = new();

        public List<string> Errors { get; } = new();

        public void Warn(string message) => Warnings.Add(message);

        public void Error(string message) => Errors.Add(message);
    }

    /// <summary>Политика, заданная тестом: библиотека своих кодов не знает, знает их тест.</summary>
    public sealed class ScriptedPolicy : IQueueRetryPolicy
    {
        private readonly Func<QueueAttempt, QueueRetryDecision> _decide;

        public ScriptedPolicy(Func<QueueAttempt, QueueRetryDecision> decide)
        {
            _decide = decide;
        }

        public List<QueueAttempt> Attempts { get; } = new();

        public QueueRetryDecision Decide(QueueAttempt attempt)
        {
            Attempts.Add(attempt);
            return _decide(attempt);
        }
    }

    public sealed class RecordingDispatcher : IQueueDispatcher<string>
    {
        private readonly Func<QueueBatch<string>, QueueResponse> _respond;

        public RecordingDispatcher(Func<QueueBatch<string>, QueueResponse> respond = null)
        {
            _respond = respond ?? (batch => QueueResponse.FromServer(Endpoint(batch), 200));
        }

        public List<QueueBatch<string>> Batches { get; } = new();

        /// <summary>Диспетчер бросает отмену — так выглядит отмена вызывающим.</summary>
        public bool ThrowsCancellation { get; set; }

        /// <summary>Диспетчер бросает необъявленное исключение — дефект самого диспетчера.</summary>
        public Exception Throws { get; set; }

        public int Calls => Batches.Count;

        public static QueueEndpoint Endpoint(QueueBatch<string> batch) =>
            new("POST", "/" + batch.Kind);

        public UniTask<QueueResponse> DeliverAsync(
            QueueBatch<string> batch, CancellationToken cancellationToken)
        {
            Batches.Add(batch);

            if (ThrowsCancellation)
            {
                throw new OperationCanceledException();
            }

            if (Throws != null)
            {
                throw Throws;
            }

            return UniTask.FromResult(_respond(batch));
        }
    }

    public sealed class RecordingTransport : IQueueTransport
    {
        private readonly Func<int, QueueResponse> _respond;

        public RecordingTransport(Func<int, QueueResponse> respond)
        {
            _respond = respond;
        }

        public int Calls { get; private set; }

        public UniTask<QueueResponse> SendAsync(
            QueueRequest request, CancellationToken cancellationToken)
        {
            QueueResponse response = _respond(Calls);
            Calls++;
            return UniTask.FromResult(response);
        }
    }

    /// <summary>
    /// Джиттер, всегда берущий верхнюю границу окна. Нужен ровно затем, чтобы РОСТ бэкоффа можно
    /// было утверждать: со случайной точкой внутри окна очередная задержка законно бывает меньше
    /// предыдущей, и тест на «стало больше» мигал бы, ничего при этом не проверяя.
    /// </summary>
    public sealed class MaxJitterRandom : Random
    {
        public override double NextDouble() => 1d;
    }

    public static class QueueTestFactory
    {
        public const string Key = "queue";

        public static DurableLaneOptions Options(
            IEnumerable<QueueStreamOptions> streams = null,
            QueueLimits limits = null,
            QueueBackoff backoff = null)
        {
            return new DurableLaneOptions(
                Key,
                limits: limits,
                streams: streams,
                backoff: backoff ?? new QueueBackoff(random: new MaxJitterRandom()));
        }

        public static DurableLane<string> Lane(
            FakeQueueStorage storage,
            IQueueDispatcher<string> dispatcher,
            IQueueRetryPolicy policy,
            IQueueClock clock,
            IQueueLog log,
            DurableLaneOptions options = null,
            IQueueCodec<string> codec = null)
        {
            return new DurableLane<string>(
                storage, dispatcher, policy, options ?? Options(), codec, clock, log);
        }
    }
}
