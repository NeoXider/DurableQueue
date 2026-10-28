using System;

namespace Neoxider.DurableQueue.Ports
{
    /// <summary>
    /// «Сейчас» в UTC. Порт по единственной причине, зато твёрдой: срок жизни записи и момент
    /// следующей попытки лежат В САМОЙ ЗАПИСИ, поэтому проверить их можно либо подменяемыми часами,
    /// либо реальным ожиданием — а тест, который ждёт неделю, никто не запустит.
    /// </summary>
    public interface IQueueClock
    {
        public DateTime UtcNow { get; }
    }

    /// <summary>Часы по умолчанию — системные.</summary>
    public sealed class SystemQueueClock : IQueueClock
    {
        public DateTime UtcNow => DateTime.UtcNow;
    }
}
