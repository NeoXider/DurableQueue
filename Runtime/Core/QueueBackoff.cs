using System;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Экспоненциальная задержка с ПОЛНЫМ джиттером: <c>delay = rand(0, min(base * 2^n, cap))</c>.
    ///
    /// <para>Джиттер полный, а не «плюс-минус процент», по причине, которая проявляется только на
    /// масштабе: после общего обрыва связи все клиенты просыпаются одновременно и повторяют залпом
    /// — ровно тогда, когда сервер и так нездоров. Случайная точка внутри окна размазывает залп.</para>
    ///
    /// <para>Потолок окна растёт, сама задержка — нет: <c>Next</c> может вернуть значение меньше
    /// предыдущего, и это нормально. Расти обязано ОКНО (<see cref="Ceiling"/>), и именно его
    /// проверяют тесты — иначе пришлось бы либо выключать джиттер, либо мириться с мигающим тестом.</para>
    /// </summary>
    public sealed class QueueBackoff
    {
        private readonly Random _random;

        public QueueBackoff(TimeSpan? baseDelay = null, TimeSpan? cap = null, Random random = null)
        {
            BaseDelay = baseDelay ?? TimeSpan.FromSeconds(2);
            Cap = cap ?? TimeSpan.FromMinutes(15);
            _random = random ?? new Random();
        }

        public TimeSpan BaseDelay { get; }

        public TimeSpan Cap { get; }

        /// <summary>Верхняя граница окна для <paramref name="failures"/> уже случившихся неудач.</summary>
        public TimeSpan Ceiling(int failures) => CeilingOf(BaseDelay, Cap, failures);

        /// <summary>
        /// То же окно без экземпляра и без случайности: <c>min(baseDelay * 2^failures, cap)</c>. Для
        /// потребителя, которому нужна сама формула роста — например, повтор заявки, которую ждёт
        /// один игрок, где размазывать залп некому.
        /// </summary>
        public static TimeSpan CeilingOf(TimeSpan baseDelay, TimeSpan cap, int failures)
        {
            // Показатель ограничен до умножения: 2^n при n порядка сотни переполняет double и
            // превращает задержку в бесконечность, а очередь — в замолчавшую навсегда.
            int exponent = Math.Clamp(failures, 0, 30);
            double milliseconds = baseDelay.TotalMilliseconds * Math.Pow(2, exponent);
            return milliseconds >= cap.TotalMilliseconds
                ? cap
                : TimeSpan.FromMilliseconds(milliseconds);
        }

        public TimeSpan Next(int failures)
        {
            double ceilingMilliseconds = Ceiling(failures).TotalMilliseconds;
            return TimeSpan.FromMilliseconds(_random.NextDouble() * ceilingMilliseconds);
        }
    }
}
