using System.Collections.Generic;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Готовый к отправке запрос немедленной полосы. Библиотека его не собирает и не знает ни одного
    /// адреса: базовый URL, схема авторизации и имена полей — знание вызывающего.
    ///
    /// <para><b>Учётных данных здесь быть не должно дольше одного вызова.</b> Заголовки приходят
    /// снаружи в момент отправки и в очередь не попадают никогда: очередь, сохранившая заголовок
    /// авторизации, превращается в хранилище долгоживущих учётных данных на устройстве
    /// пользователя.</para>
    /// </summary>
    public sealed class QueueRequest
    {
        private static readonly IReadOnlyDictionary<string, string> NoHeaders =
            new Dictionary<string, string>(0);

        public QueueRequest(
            string method,
            string url,
            string pathTemplate = null,
            IReadOnlyDictionary<string, string> headers = null,
            string body = null)
        {
            Method = method;
            Url = url;
            Endpoint = new QueueEndpoint(method, pathTemplate ?? url);
            Headers = headers ?? NoHeaders;
            Body = body;
        }

        public string Method { get; }

        public string Url { get; }

        /// <summary>Адрес ручки для политики повторов — см. <see cref="QueueEndpoint"/>.</summary>
        public QueueEndpoint Endpoint { get; }

        public IReadOnlyDictionary<string, string> Headers { get; }

        public string Body { get; }
    }
}
