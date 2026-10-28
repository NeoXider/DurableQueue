namespace Neoxider.DurableQueue
{
    public enum QueueEnqueueStatus
    {
        Accepted = 0,

        /// <summary>Запись больше капа на одну запись.</summary>
        RejectedTooLarge = 1,

        /// <summary>
        /// Хранилище записать отказалось. Последняя инстанция: сначала очередь вытесняет всё, что
        /// можно вытеснить, потом соглашается превысить мягкий бюджет — и только физический отказ
        /// носителя превращается в отказ постановки.
        /// </summary>
        RejectedStorageFailure = 2,

        /// <summary>
        /// Хранилище отказало, но поток разрешает держать запись в памяти
        /// (<see cref="QueueStreamOptions.RetainWhenStorageRefuses"/>): запись будет доставлена в этом
        /// запуске, перезапуск она не переживёт, пока носитель её не примет.
        /// </summary>
        AcceptedInMemoryOnly = 3
    }

    /// <summary>
    /// Исход постановки. Возвращается, а не бросается: постановка происходит в кадре обычной работы
    /// приложения, и исключение оттуда прилетает тому, кто «просто записал факт».
    ///
    /// <para><b>Принято ≠ доставлено.</b> <see cref="Accepted"/> означает ровно одно: запись
    /// сохранена локально и попыток доставки не оставит. Ждать по ней ответа сервера нельзя — в
    /// этом и смысл долговечной полосы.</para>
    /// </summary>
    public readonly struct QueueEnqueueResult
    {
        private QueueEnqueueResult(QueueEnqueueStatus status, string reason, bool overBudget)
        {
            Status = status;
            Reason = reason;
            OverBudget = overBudget;
        }

        public QueueEnqueueStatus Status { get; }

        public string Reason { get; }

        /// <summary>
        /// Запись принята, но мягкий бюджет превышен: вытеснять было нечего, а терять — нельзя.
        /// Превышение бюджета — внутренняя проблема очереди, потеря данных — проблема пользователя,
        /// поэтому выбор сделан в эту сторону; молчать при этом нельзя.
        /// </summary>
        public bool OverBudget { get; }

        /// <summary>Запись в очереди и будет доставляться — на носителе или только в памяти.</summary>
        public bool Accepted =>
            Status == QueueEnqueueStatus.Accepted || Status == QueueEnqueueStatus.AcceptedInMemoryOnly;

        /// <summary>Запись переживёт перезапуск.</summary>
        public bool IsPersisted => Status == QueueEnqueueStatus.Accepted;

        public static QueueEnqueueResult Ok(bool overBudget = false) =>
            new(QueueEnqueueStatus.Accepted, null, overBudget);

        public static QueueEnqueueResult TooLarge(string reason) =>
            new(QueueEnqueueStatus.RejectedTooLarge, reason, false);

        public static QueueEnqueueResult StorageFailure(string reason) =>
            new(QueueEnqueueStatus.RejectedStorageFailure, reason, false);

        public static QueueEnqueueResult InMemoryOnly(string reason) =>
            new(QueueEnqueueStatus.AcceptedInMemoryOnly, reason, false);
    }
}
