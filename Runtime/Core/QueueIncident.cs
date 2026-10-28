using System;

namespace Neoxider.DurableQueue
{
    public enum QueueIncidentReason
    {
        /// <summary>Политика сказала «повтор не поможет никогда».</summary>
        PermanentRejection = 0,

        /// <summary>Потолок ответов сервера исчерпан.</summary>
        AttemptsExhausted = 1,

        /// <summary>Срок жизни записи истёк.</summary>
        Expired = 2,

        /// <summary>Запись вытеснена при переполнении.</summary>
        Evicted = 3,

        /// <summary>Мягкий бюджет превышен, но запись принята.</summary>
        OverBudget = 4,

        /// <summary>Хранилище отказалось писать.</summary>
        StorageRefused = 5,

        /// <summary>Запись больше капа на одну запись и в очередь не принята.</summary>
        TooLarge = 6
    }

    /// <summary>
    /// Происшествие в очереди — единственный способ узнать о потере, кроме журнала.
    ///
    /// <para>Событие существует потому, что молчаливая потеря и есть та беда, ради которой пишут
    /// очередь: снаружи «данные ещё едут» и «данные исчезли» выглядят одинаково — пусто. Приложение
    /// подписывается и решает, что показать: плашку, счётчик в диагностике или ничего.</para>
    ///
    /// <para><b>Содержимого записи здесь нет и не будет.</b> Наблюдаемость нужна метаданным.</para>
    /// </summary>
    public sealed class QueueIncident
    {
        public QueueIncident(
            QueueIncidentReason reason,
            string stream,
            string kind,
            string idempotencyKey,
            int statusCode,
            string detail,
            int recordCount = 1,
            int bytes = 0,
            DateTime createdAtUtc = default)
        {
            Reason = reason;
            Stream = stream;
            Kind = kind;
            IdempotencyKey = idempotencyKey;
            StatusCode = statusCode;
            Detail = detail;
            RecordCount = recordCount;
            Bytes = bytes;
            CreatedAtUtc = createdAtUtc;
        }

        public QueueIncidentReason Reason { get; }

        public string Stream { get; }

        public string Kind { get; }

        public string IdempotencyKey { get; }

        public int StatusCode { get; }

        public string Detail { get; }

        public int RecordCount { get; }

        public int Bytes { get; }

        public DateTime CreatedAtUtc { get; }
    }
}
