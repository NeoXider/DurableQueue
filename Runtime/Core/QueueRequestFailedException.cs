using System;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Отказ немедленной полосы. Исключение, а не пустой результат, — это главное, что немедленная
    /// полоса вообще утверждает: вызывающий ждёт ответ прямо сейчас, и «ничего не пришло, но всё
    /// хорошо» — это и есть тот молчаливый отказ, из-за которого данные пропадают незамеченными.
    /// </summary>
    public sealed class QueueRequestFailedException : Exception
    {
        public QueueRequestFailedException(string reason, QueueResponse response)
            : base(BuildMessage(reason, response))
        {
            Reason = reason;
            Response = response;
        }

        public string Reason { get; }

        public QueueResponse Response { get; }

        public int StatusCode => Response?.StatusCode ?? 0;

        private static string BuildMessage(string reason, QueueResponse response)
        {
            string endpoint = response == null ? "unknown endpoint" : response.Endpoint.ToString();
            int status = response?.StatusCode ?? 0;
            return $"Request to {endpoint} failed: {reason ?? "no reason given"} (status {status})";
        }
    }
}
