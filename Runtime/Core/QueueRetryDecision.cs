using System;

namespace Neoxider.DurableQueue
{
    public enum QueueRetryAction
    {
        /// <summary>Считать доставленным и снять с очереди.</summary>
        Done = 0,

        /// <summary>Оставить в голове потока и попробовать позже.</summary>
        RetryLater = 1,

        /// <summary>
        /// Снять с очереди без доставки: правило предметной области нарушено, повтор не поможет
        /// никогда. Исход обязан дойти до того, кто ставил запись, — молчаливое снятие здесь и есть
        /// потеря данных без следа.
        /// </summary>
        Drop = 2,

        /// <summary>
        /// Приостановить долговечную полосу целиком: не «эта запись плоха», а «сейчас нельзя
        /// отправлять ничего» (типичная причина — подтверждение личности протухло). Попытка не
        /// расходуется, ни одна запись не выбрасывается.
        /// </summary>
        Suspend = 3
    }

    /// <summary>
    /// Ответ политики. Форма ответа — библиотечная, а СПИСОК КОДОВ — нет: у другого сервера коды
    /// другие, и зашитый список сделал бы библиотеку чужой собственностью на первой же строке.
    /// </summary>
    public readonly struct QueueRetryDecision
    {
        private QueueRetryDecision(QueueRetryAction action, TimeSpan? delay, string reason, bool spendsAttempt)
        {
            Action = action;
            Delay = delay;
            Reason = reason;
            SpendsAttempt = spendsAttempt;
        }

        public QueueRetryAction Action { get; }

        /// <summary>
        /// Явная задержка. <c>null</c> — «решай сам»: очередь возьмёт свой бэкофф. Явная нужна там,
        /// где её назвал сервер: сказанное им «повторить через N» важнее нашей арифметики.
        /// </summary>
        public TimeSpan? Delay { get; }

        /// <summary>Краткая машинная причина для журнала и dead-letter. Без содержимого записи.</summary>
        public string Reason { get; }

        /// <summary>
        /// Расходует ли повтор потолок попыток. <c>false</c> — «сервер ответил, но ответ временный, и
        /// судить запись по нему нельзя»: у идемпотентного получателя 5xx или 429 не повод когда-либо
        /// сдаться. Бэкофф при этом растёт так же, как от любой другой неудачи.
        /// </summary>
        public bool SpendsAttempt { get; }

        public static QueueRetryDecision Done() => new(QueueRetryAction.Done, null, null, false);

        public static QueueRetryDecision RetryLater(
            TimeSpan? delay = null,
            string reason = null,
            bool spendsAttempt = true) =>
            new(QueueRetryAction.RetryLater, delay, reason, spendsAttempt);

        public static QueueRetryDecision Drop(string reason) =>
            new(QueueRetryAction.Drop, null, reason ?? "permanent", false);

        public static QueueRetryDecision Suspend(string reason = null) =>
            new(QueueRetryAction.Suspend, null, reason ?? "suspended", false);
    }
}
