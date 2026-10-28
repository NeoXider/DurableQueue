using System;
using System.Collections.Generic;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Исход одной отправки — один и тот же тип у транспорта и у диспетчера. Единая форма нужна
    /// затем, чтобы политика повторов получала ОДИНАКОВЫЙ вход независимо от того, как именно
    /// уехала запись: иначе типизированный клиент, который бросает исключение вместо кода ответа,
    /// просто не доносит до классификации свой отказ, и тот молча становится «неизвестным кодом».
    /// </summary>
    public sealed class QueueResponse
    {
        private static readonly IReadOnlyDictionary<string, string> NoHeaders =
            new Dictionary<string, string>(0);

        private QueueResponse(
            QueueEndpoint endpoint,
            int statusCode,
            IReadOnlyDictionary<string, string> headers,
            string body,
            bool transportFailure,
            string error)
        {
            Endpoint = endpoint;
            StatusCode = statusCode;
            Headers = headers ?? NoHeaders;
            Body = body;
            TransportFailure = transportFailure;
            Error = error;
        }

        public QueueEndpoint Endpoint { get; }

        /// <summary>Код ответа сервера. <c>0</c> — сервер не ответил вовсе.</summary>
        public int StatusCode { get; }

        /// <summary>
        /// Заголовки ответа. Не украшение: указание «повторить через N» и валидаторы кэша приезжают
        /// заголовками, и без них политика их просто не увидит.
        /// </summary>
        public IReadOnlyDictionary<string, string> Headers { get; }

        public string Body { get; }

        /// <summary>
        /// Сервер не ответил: обрыв, таймаут, ошибка стека. Признак отделён от кода намеренно —
        /// от него зависит, расходуется ли потолок попыток. Час без сети не должен исчерпать попытки
        /// и увести запись в dead-letter: сервер ничего не сказал, значит и судить не о чем.
        /// </summary>
        public bool TransportFailure { get; }

        /// <summary>Краткий текст ошибки для журнала. Содержимого записи здесь быть не должно.</summary>
        public string Error { get; }

        public static QueueResponse FromServer(QueueEndpoint endpoint, int statusCode, IReadOnlyDictionary<string, string> headers = null, string body = null) => new(endpoint, statusCode, headers, body, false, null);

        public static QueueResponse FromTransportFailure(QueueEndpoint endpoint, string error) => new(endpoint, 0, null, null, true, error);

        /// <summary>
        /// Необъявленное исключение отправляющей стороны. Отдельная фабрика существует ради
        /// единственного правила: неучтённое исключение — это «сервер не ответил», а не «сервер
        /// отказал», и потолок попыток оно тратить не имеет права.
        /// </summary>
        public static QueueResponse FromException(QueueEndpoint endpoint, Exception exception) => FromTransportFailure(endpoint, exception?.GetType().Name ?? "exception");
    }
}
