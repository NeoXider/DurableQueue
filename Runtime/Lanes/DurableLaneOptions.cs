using System;
using System.Collections.Generic;

namespace Neoxider.DurableQueue.Lanes
{
    /// <summary>
    /// Настройка долговечной полосы. Всё, что здесь есть, — знание приложения: где хранить, чем
    /// жертвовать при переполнении и сколько терпеть. Значений «на всякий случай» здесь нет: лишняя
    /// ручка настройки — это ещё один способ настроить неправильно.
    /// </summary>
    public sealed class DurableLaneOptions
    {
        public DurableLaneOptions(
            string recordsKey,
            string deadLetterKey = null,
            QueueLimits limits = null,
            IEnumerable<QueueStreamOptions> streams = null,
            QueueBackoff backoff = null,
            IEnumerable<string> knownKinds = null)
        {
            RecordsKey = recordsKey;
            if (knownKinds != null)
            {
                _knownKinds = new HashSet<string>(knownKinds, StringComparer.Ordinal);
            }

            DeadLetterKey = deadLetterKey ?? recordsKey + ".dead-letter";
            Limits = limits ?? new QueueLimits();
            Streams = new QueueStreamRegistry(streams);
            Backoff = backoff ?? new QueueBackoff();
        }

        public string RecordsKey { get; }

        /// <summary>
        /// Отдельный ключ для недоставленного. Отдельный намеренно: он обязан пережить очистку самой
        /// очереди, потому что вопрос «куда делась запись» задают уже после того, как записи не стало.
        /// </summary>
        public string DeadLetterKey { get; }

        public QueueLimits Limits { get; }

        public QueueStreamRegistry Streams { get; }

        public QueueBackoff Backoff { get; }

        private readonly HashSet<string> _knownKinds;

        /// <summary>
        /// Знает ли приложение, как доставить запись этого вида. Незнакомый вид (запись оставила более
        /// новая версия приложения) хранится, но не отправляется и никого не задерживает: доставит его
        /// та версия, которая его знает. <c>null</c> в настройке — «знакомы все виды».
        /// </summary>
        public bool IsKnownKind(string kind) => _knownKinds == null || (kind != null && _knownKinds.Contains(kind));
    }
}
