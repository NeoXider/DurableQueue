using System.Threading;
using Cysharp.Threading.Tasks;

namespace Neoxider.DurableQueue.Ports
{
    /// <summary>
    /// Отправка одного готового запроса. Порт, потому что HTTP-клиент — тип движка и слоя
    /// приложения, а подменяемый транспорт заодно делает тесты по-настоящему офлайновыми.
    ///
    /// <para>Реализация обязана вернуть <see cref="QueueResponse"/> и в случае неудачи тоже:
    /// исключение вместо ответа лишает политику входа и превращает отказ в «неизвестный код».
    /// Исключение отмены (<see cref="System.OperationCanceledException"/>) — единственное, которое
    /// проходит наружу: отмена вызывающим не ошибка доставки.</para>
    /// </summary>
    public interface IQueueTransport
    {
        public UniTask<QueueResponse> SendAsync(QueueRequest request, CancellationToken cancellationToken);
    }
}
