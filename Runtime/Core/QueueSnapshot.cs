using System;
using System.Collections.Generic;

namespace Neoxider.DurableQueue
{
    /// <summary>Состояние одного потока на момент снимка.</summary>
    public sealed class QueueStreamSnapshot
    {
        public QueueStreamSnapshot(
            string stream,
            int count,
            string headKind,
            bool headHasTarget,
            DateTime? headNextAttemptAtUtc,
            int headAttempts,
            int headTransportFailures)
        {
            Stream = stream;
            Count = count;
            HeadKind = headKind;
            HeadHasTarget = headHasTarget;
            HeadNextAttemptAtUtc = headNextAttemptAtUtc;
            HeadAttempts = headAttempts;
            HeadTransportFailures = headTransportFailures;
        }

        public string Stream { get; }

        public int Count { get; }

        public string HeadKind { get; }

        /// <summary>
        /// Есть ли у головы адрес. Отдельным полем, потому что «стоит без адреса» и «стоит из-за
        /// ошибки» — разные диагнозы, а снаружи оба выглядят как «очередь не движется».
        /// </summary>
        public bool HeadHasTarget { get; }

        public DateTime? HeadNextAttemptAtUtc { get; }

        public int HeadAttempts { get; }

        public int HeadTransportFailures { get; }
    }

    /// <summary>
    /// Снимок очереди как ДАННЫЕ. Библиотека ничего не рисует: как показать это пользователю —
    /// решает приложение. Но не дать посмотреть она не имеет права: очередь, состояние которой
    /// нельзя увидеть, отличается от потерянных данных только верой.
    /// </summary>
    public sealed class QueueSnapshot
    {
        public QueueSnapshot(
            int count,
            int bytes,
            bool isLoaded,
            DateTime? suspendedUntilUtc,
            IReadOnlyList<QueueStreamSnapshot> streams,
            IReadOnlyList<QueueDeadLetterEntry> deadLetter)
        {
            Count = count;
            Bytes = bytes;
            IsLoaded = isLoaded;
            SuspendedUntilUtc = suspendedUntilUtc;
            Streams = streams;
            DeadLetter = deadLetter;
        }

        public int Count { get; }

        public int Bytes { get; }

        /// <summary><c>false</c> — сохранённое ещё не прочитано, ключа пока нет.</summary>
        public bool IsLoaded { get; }

        public DateTime? SuspendedUntilUtc { get; }

        public IReadOnlyList<QueueStreamSnapshot> Streams { get; }

        public IReadOnlyList<QueueDeadLetterEntry> DeadLetter { get; }
    }
}
