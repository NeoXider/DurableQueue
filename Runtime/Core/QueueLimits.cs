using System;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Границы объёма и терпения. Значения по умолчанию консервативны: превысить их дешевле, чем
    /// однажды упереться в потолок хранилища и потерять всё сразу.
    /// </summary>
    public sealed class QueueLimits
    {
        /// <summary>
        /// Мягкий бюджет всей очереди в БАЙТАХ сериализованного вида, а не в числе записей.
        /// Ограничение по числу записей ничего не гарантирует: тысяча длинных записей — это
        /// мегабайты, а тысяча коротких — десятки килобайт.
        /// </summary>
        public int TotalBudgetBytes { get; set; } = 256 * 1024;

        /// <summary>
        /// Кап на одну запись. Больше — не в очередь, а видимый отказ постановки: молча урезанная
        /// нагрузка хуже отказа, потому что отказ виден сразу, а урезание обнаружится у получателя.
        /// </summary>
        public int MaxRecordBytes { get; set; } = 32 * 1024;

        /// <summary>
        /// Срок жизни записи. Исход у классов записей РАЗНЫЙ: протухший снимок просто снимается
        /// (он хуже отсутствующего — может перезаписать более свежее состояние), а протухший факт
        /// уходит в dead-letter, потому что истечение срока — это потеря факта, и молчать о ней
        /// нельзя.
        /// </summary>
        public TimeSpan Ttl { get; set; } = TimeSpan.FromDays(7);

        /// <summary>
        /// Потолок попыток. Считает ТОЛЬКО ответы сервера: двенадцать подряд повторяемых ответов —
        /// это устойчивый отказ, а не «сеть моргнула». Отсутствие сети попыток не расходует.
        /// </summary>
        public int MaxAttempts { get; set; } = 12;

        /// <summary>
        /// Через сколько приостановка снимается сама. Страховка от забытого возобновления:
        /// приостановленную навсегда очередь снаружи не отличить от пустой, и обнаруживается это
        /// пропажей данных, а не ошибкой.
        /// </summary>
        public TimeSpan SuspendCeiling { get; set; } = TimeSpan.FromMinutes(10);

        /// <summary>Сколько последних неудач помнить в dead-letter.</summary>
        public int DeadLetterCap { get; set; } = 20;
    }

    /// <summary>Насколько громко сообщать о вытеснении записей потока.</summary>
    public enum QueueEvictionSeverity
    {
        Warning = 0,
        Error = 1
    }

    /// <summary>
    /// Что известно о потоке. Всё здесь — знание приложения, а не библиотеки: цену потери данных
    /// назначает тот, кто знает, что это за данные.
    /// </summary>
    public sealed class QueueStreamOptions
    {
        public QueueStreamOptions(
            string stream,
            bool evictable = false,
            int evictionPriority = 0,
            QueueEvictionSeverity evictionSeverity = QueueEvictionSeverity.Warning,
            bool flushOnWrite = false,
            bool neverExpires = false,
            bool retainWhenStorageRefuses = false,
            int maxBatchSize = 0,
            bool independentTargets = false)
        {
            Stream = stream;
            Evictable = evictable;
            EvictionPriority = evictionPriority;
            EvictionSeverity = evictionSeverity;
            FlushOnWrite = flushOnWrite;
            NeverExpires = neverExpires;
            RetainWhenStorageRefuses = retainWhenStorageRefuses;
            MaxBatchSize = maxBatchSize < 0 ? 0 : maxBatchSize;
            IndependentTargets = independentTargets;
        }

        public string Stream { get; }

        /// <summary>
        /// Можно ли жертвовать записями этого потока при переполнении. По умолчанию НЕЛЬЗЯ, и
        /// умолчание выбрано в эту сторону намеренно: незарегистрированный поток иначе молча
        /// оказался бы расходным материалом, и обнаружилось бы это потерей чьей-то работы.
        /// </summary>
        public bool Evictable { get; }

        /// <summary>Кем жертвуем раньше: меньше значение — раньше вытесняется.</summary>
        public int EvictionPriority { get; }

        /// <summary>
        /// Вытеснение обязано быть слышным. Поток, который «в принципе можно потерять, но не
        /// хотелось», сообщает о себе ошибкой, а не предупреждением, — иначе решение пожертвовать им
        /// принимается один раз в коде и больше никогда не пересматривается.
        /// </summary>
        public QueueEvictionSeverity EvictionSeverity { get; }

        /// <summary>
        /// Сбрасывать ли на носитель сразу при постановке. Дорого, поэтому только там, где потеря
        /// последних секунд недопустима.
        /// </summary>
        public bool FlushOnWrite { get; }

        /// <summary>
        /// Срок жизни (<see cref="QueueLimits.Ttl"/>) к потоку не применяется. Для фактов, которые
        /// получатель применяет идемпотентно и которые нельзя терять ни через неделю, ни через месяц:
        /// запись, до которой сеть не дотянулась, должна дождаться сети, а не таймера.
        /// </summary>
        public bool NeverExpires { get; }

        /// <summary>
        /// Если хранилище физически отказало в записи, запись всё равно остаётся в очереди — в памяти
        /// — и доставляется в этом запуске; постановка возвращает
        /// <see cref="QueueEnqueueStatus.AcceptedInMemoryOnly"/>. Каждое следующее сохранение очереди
        /// снова пробует записать её на носитель. Для потоков, где «доставить без гарантии перезапуска»
        /// лучше, чем «не доставить вовсе».
        /// </summary>
        public bool RetainWhenStorageRefuses { get; }

        /// <summary>
        /// Сколько записей самое большее уходит одной пачкой; <c>0</c> — без предела. Нужен там, где
        /// у получателя есть потолок запроса и где исход пачки обязан относиться ровно к тому, что
        /// ушло одним запросом.
        /// </summary>
        public int MaxBatchSize { get; }

        /// <summary>
        /// Строгий FIFO держится внутри одного адреса (<c>target</c>), а не всего потока: запись,
        /// ждущая повтора, задерживает только записи того же адреса. Для потоков, где адрес — это
        /// независимая сущность (у каждой свой ресурс на сервере), и застрявшая одна не должна держать
        /// остальные.
        /// </summary>
        public bool IndependentTargets { get; }
    }
}
