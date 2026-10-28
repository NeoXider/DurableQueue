using System.Collections.Generic;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Что приложение рассказало о своих потоках. Незнакомый поток считается НЕПРИКОСНОВЕННЫМ:
    /// умолчание выбрано в пользу сохранности, потому что цена ошибки несимметрична — лишний
    /// перерасход места виден в диагностике, а молча выброшенная чужая работа не видна нигде.
    /// </summary>
    public sealed class QueueStreamRegistry
    {
        private static readonly QueueStreamOptions Protected = new(null);

        private readonly Dictionary<string, QueueStreamOptions> _streams;

        public QueueStreamRegistry(IEnumerable<QueueStreamOptions> streams = null)
        {
            _streams = new Dictionary<string, QueueStreamOptions>();
            if (streams == null)
            {
                return;
            }

            foreach (QueueStreamOptions options in streams)
            {
                if (options != null && !string.IsNullOrEmpty(options.Stream))
                {
                    _streams[options.Stream] = options;
                }
            }
        }

        public QueueStreamOptions Get(string stream)
        {
            if (stream != null && _streams.TryGetValue(stream, out QueueStreamOptions options))
            {
                return options;
            }

            return Protected;
        }
    }
}
