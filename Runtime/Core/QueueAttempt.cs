using System;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Всё, чем политика повторов располагает при решении: конверт, адрес ручки и ответ целиком.
    ///
    /// <para><b>Почему не одного кода ответа.</b> Один и тот же код на разных ручках значит разное,
    /// а на одной и той же ручке — разное по причине: где-то «запись уже есть» (повтор безопасен),
    /// где-то «откачена вся пачка» (снять с очереди = уничтожить данные), где-то «правило предметной
    /// области нарушено» (повтор не поможет никогда, и об этом надо сказать пользователю). Отличить
    /// эти случаи по одному числу нельзя, поэтому политика видит и ручку, и конверт, и заголовки.</para>
    /// </summary>
    public sealed class QueueAttempt
    {
        public QueueAttempt(
            string kind,
            string stream,
            string target,
            QueueRecordClass recordClass,
            int attempts,
            int transportFailures,
            DateTime createdAtUtc,
            QueueEndpoint endpoint,
            QueueResponse response)
        {
            Kind = kind;
            Stream = stream;
            Target = target;
            RecordClass = recordClass;
            Attempts = attempts;
            TransportFailures = transportFailures;
            CreatedAtUtc = createdAtUtc;
            Endpoint = endpoint;
            Response = response;
        }

        public string Kind { get; }

        public string Stream { get; }

        public string Target { get; }

        public QueueRecordClass RecordClass { get; }

        /// <summary>Сколько раз сервер уже отвечал по этой записи.</summary>
        public int Attempts { get; }

        /// <summary>Сколько раз сервер не ответил вовсе.</summary>
        public int TransportFailures { get; }

        public DateTime CreatedAtUtc { get; }

        public QueueEndpoint Endpoint { get; }

        public QueueResponse Response { get; }
    }
}
