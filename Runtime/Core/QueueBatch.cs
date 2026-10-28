using System;
using System.Collections.Generic;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Ведущий отрезок одного потока: подряд идущие записи одного вида и с одним адресом. Пачка, а
    /// не одна запись, потому что иначе теряется уже работающее свойство — «N записей уезжают одним
    /// запросом»; восстанавливать его потом дороже, чем сохранить сразу.
    ///
    /// <para>Отрезок обрывается на первой записи с другим видом или другим адресом: смешать их в
    /// один запрос нельзя, а переставить местами — значит нарушить FIFO.</para>
    /// </summary>
    public sealed class QueueBatch<T>
    {
        public QueueBatch(string stream, string kind, string target, IReadOnlyList<QueueRecord<T>> records)
        {
            Stream = stream;
            Kind = kind;
            Target = target;
            Records = records;
        }

        public string Stream { get; }

        public string Kind { get; }

        public string Target { get; }

        public IReadOnlyList<QueueRecord<T>> Records { get; }

        public QueueRecord<T> Head => Records[0];

        private List<QueueRejection<T>> _rejections;
        private List<QueueRecord<T>> _confirmed;

        /// <summary>Записи пачки, доставку которых получатель подтвердил по отдельности.</summary>
        public IReadOnlyList<QueueRecord<T>> Confirmed =>
            (IReadOnlyList<QueueRecord<T>>)_confirmed ?? Array.Empty<QueueRecord<T>>();

        /// <summary>
        /// Отмечает ОДНУ запись пачки как доставленную — независимо от общего исхода. Для диспетчера,
        /// который отправляет пачку частями: уже принятая часть снимается с очереди сразу, и прогресс
        /// не теряется, даже если остаток пачки ждёт следующего прохода.
        /// </summary>
        public void Confirm(QueueRecord<T> record)
        {
            if (record == null || !Contains(record) || IsRejected(record))
            {
                return;
            }

            _confirmed ??= new List<QueueRecord<T>>();
            if (!_confirmed.Contains(record))
            {
                _confirmed.Add(record);
            }
        }

        /// <summary>Записи пачки, которые получатель отверг по отдельности.</summary>
        public IReadOnlyList<QueueRejection<T>> Rejections =>
            (IReadOnlyList<QueueRejection<T>>)_rejections ?? Array.Empty<QueueRejection<T>>();

        /// <summary>
        /// Отмечает ОДНУ запись пачки как отвергнутую навсегда. Для диспетчера, который умеет выяснить,
        /// какая именно запись пачки не принимается (например, делением пачки пополам), — чтобы одна
        /// ядовитая запись не утянула за собой в dead-letter исправных соседей.
        ///
        /// <para>Отмеченная запись снимается с очереди и оставляет строку в dead-letter и происшествие
        /// независимо от общего исхода пачки; к остальным записям исход применяется как обычно.</para>
        /// </summary>
        public void Reject(QueueRecord<T> record, int statusCode, string reason, string error = null)
        {
            if (record == null || !Contains(record) || (_confirmed != null && _confirmed.Contains(record)))
            {
                return;
            }

            _rejections ??= new List<QueueRejection<T>>();
            for (int i = 0; i < _rejections.Count; i++)
            {
                if (ReferenceEquals(_rejections[i].Record, record))
                {
                    return;
                }
            }

            _rejections.Add(new QueueRejection<T>(record, statusCode, reason, error));
        }

        private bool IsRejected(QueueRecord<T> record)
        {
            if (_rejections == null)
            {
                return false;
            }

            for (int i = 0; i < _rejections.Count; i++)
            {
                if (ReferenceEquals(_rejections[i].Record, record))
                {
                    return true;
                }
            }

            return false;
        }

        private bool Contains(QueueRecord<T> record)
        {
            for (int i = 0; i < Records.Count; i++)
            {
                if (ReferenceEquals(Records[i], record))
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>Отказ получателя по одной записи пачки.</summary>
    public sealed class QueueRejection<T>
    {
        public QueueRejection(QueueRecord<T> record, int statusCode, string reason, string error)
        {
            Record = record;
            StatusCode = statusCode;
            Reason = reason;
            Error = error;
        }

        public QueueRecord<T> Record { get; }

        public int StatusCode { get; }

        /// <summary>Краткая машинная причина. Без содержимого записи.</summary>
        public string Reason { get; }

        public string Error { get; }
    }
}
