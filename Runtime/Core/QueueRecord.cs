using System;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Класс записи. Различие несущее: без него очередь либо раздувается (каждое промежуточное
    /// значение отдельной записью), либо теряет факты (схлопывание там, где схлопывать нельзя).
    /// </summary>
    public enum QueueRecordClass
    {
        /// <summary>
        /// Факт, который случился один раз и не отменяется: снимается только по подтверждению,
        /// строгий FIFO внутри потока, не схлопывается никогда.
        /// </summary>
        Event = 0,

        /// <summary>
        /// Снимок, у которого важно только последнее значение: новая запись вытесняет старую,
        /// глубина потока всегда 1.
        /// </summary>
        State = 1
    }

    /// <summary>
    /// Конверт записи. Полей ровно столько, сколько нужно, чтобы пережить перезапуск и принять
    /// решение о повторе БЕЗ участия вызывающего.
    /// </summary>
    /// <typeparam name="T">Тип полезной нагрузки; библиотека её не интерпретирует.</typeparam>
    public sealed class QueueRecord<T>
    {
        public string Id { get; set; }

        /// <summary>Вид записи. Смысл вида знает вызывающий; очередь им только группирует пачки.</summary>
        public string Kind { get; set; }

        /// <summary>
        /// Поток. Внутри потока — строгий FIFO с head-of-line blocking, между потоками —
        /// независимость. Глобального порядка нет намеренно: одна ядовитая запись иначе заблокирует
        /// всё, включая то, терять что нельзя.
        /// </summary>
        public string Stream { get; set; }

        public QueueRecordClass Class { get; set; }

        /// <summary>
        /// Ключ идемпотентности. Вычисляет его вызывающий и только он: правило вывода — предметное
        /// (у факта одно, у снимка другое), и внутрь библиотеки оно не попадает.
        /// </summary>
        public string IdempotencyKey { get; set; }

        /// <summary>
        /// Адрес доставки, зафиксированный В МОМЕНТ ПОСТАНОВКИ.
        ///
        /// <para>Это не оптимизация, а починка реального дефекта: пока адрес разрешался в момент
        /// отправки, он успевал смениться между постановкой и удачной доставкой, и хвост записей
        /// уезжал не туда, куда его писали. Пустой адрес — законное состояние («адреса ещё нет»),
        /// а не устаревший: такие записи насос не отправляет и попыткой это не считает, а адрес им
        /// ставит один раз <c>AssignTargetToUnaddressed</c>.</para>
        /// </summary>
        public string Target { get; set; }

        public T Payload { get; set; }

        public DateTime CreatedAtUtc { get; set; }

        /// <summary>
        /// Сколько раз сервер ОТВЕТИЛ что-то повторяемое. Только этот счётчик расходует потолок
        /// попыток.
        /// </summary>
        public int Attempts { get; set; }

        /// <summary>
        /// Сколько раз сервер не ответил вовсе. Двигает бэкофф, но потолка не касается: иначе час
        /// без сети сам стал бы механизмом потери записи.
        /// </summary>
        public int TransportFailures { get; set; }

        /// <summary>
        /// Сколько раз сервер ответил временным отказом, который по решению политики потолок попыток
        /// НЕ расходует (<see cref="QueueRetryDecision.SpendsAttempt"/>). Двигает только бэкофф.
        /// </summary>
        public int FreeRetries { get; set; }

        /// <summary>
        /// Когда можно пробовать снова. Лежит В ЗАПИСИ, а не в таймере, — иначе перезапуск
        /// сбрасывает бэкофф в ноль и очередь превращается в шторм на старте.
        /// </summary>
        public DateTime? NextAttemptAtUtc { get; set; }

        /// <summary>Краткий признак прошлой неудачи для журнала. Содержимого записи здесь нет.</summary>
        public string LastError { get; set; }

        public bool HasTarget => !string.IsNullOrEmpty(Target);

        /// <summary>Суммарное число неудач — показатель степени бэкоффа.</summary>
        public int Failures => Attempts + TransportFailures + FreeRetries;

        public bool IsDue(DateTime utcNow) => !NextAttemptAtUtc.HasValue || NextAttemptAtUtc.Value <= utcNow;
    }
}
