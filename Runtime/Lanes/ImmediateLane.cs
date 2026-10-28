using System;
using System.Threading;
using Cysharp.Threading.Tasks;
using Neoxider.DurableQueue.Ports;

namespace Neoxider.DurableQueue.Lanes
{
    /// <summary>
    /// Немедленная полоса: ответ нужен сейчас и потеря терпима.
    ///
    /// <para><b>Отказ здесь виден вызывающему — это её главное свойство.</b> Отложить чтение
    /// бессмысленно: экрану нужен ответ, а не обещание, и «пусто, но всё хорошо» — это тот самый
    /// молчаливый отказ, из-за которого проблемы обнаруживаются через недели. Поэтому исчерпание
    /// повторов и постоянный отказ заканчиваются исключением.</para>
    ///
    /// <para><b>Приостановка ей не назначается никогда, и это осознанно.</b> Через немедленную
    /// полосу идёт в том числе восстановление доступа — то есть ровно то, чем приостановка
    /// снимается. Запрети её здесь, и приостановка станет дверью, запертой снаружи: снять её нечем.
    /// Поэтому просьба политики приостановиться превращается для этой полосы в видимую ошибку, а
    /// что с ней делать — решает вызывающий.</para>
    /// </summary>
    public sealed class ImmediateLane
    {
        private readonly IQueueTransport _transport;
        private readonly IQueueRetryPolicy _policy;
        private readonly IQueueLog _log;
        private readonly QueueBackoff _backoff;
        private readonly int _maxAttempts;

        public ImmediateLane(
            IQueueTransport transport,
            IQueueRetryPolicy policy,
            QueueBackoff backoff = null,
            IQueueLog log = null,
            int maxAttempts = 4)
        {
            _transport = transport;
            _policy = policy;
            _backoff = backoff ?? new QueueBackoff();
            _log = log;
            _maxAttempts = Math.Max(1, maxAttempts);
        }

        /// <summary>
        /// Отправляет запрос, повторяя по решению политики в пределах ОДНОГО вызова.
        ///
        /// <para>Потолок повторов здесь свой и маленький: вызывающий ждёт, и «ещё разок через
        /// пятнадцать минут» для него не помощь, а зависание. По той же причине потолок расходуют
        /// ОБА счётчика — и ответы сервера, и обрывы связи: у долговечной полосы отсутствие сети
        /// попыток не тратит, потому что там никто не ждёт, а здесь ждут.</para>
        /// </summary>
        /// <exception cref="QueueRequestFailedException">
        /// Повторы исчерпаны, отказ постоянный или политика попросила приостановиться.
        /// </exception>
        public async UniTask<QueueResponse> SendAsync(
            QueueRequest request, CancellationToken cancellationToken = default)
        {
            DateTime startedAtUtc = DateTime.UtcNow;
            int attempts = 0;
            int transportFailures = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                QueueResponse response = await _transport.SendAsync(request, cancellationToken);
                QueueAttempt attempt = new(
                    null,
                    null,
                    null,
                    QueueRecordClass.Event,
                    attempts,
                    transportFailures,
                    startedAtUtc,
                    request.Endpoint,
                    response);

                QueueRetryDecision decision = _policy.Decide(attempt);
                switch (decision.Action)
                {
                    case QueueRetryAction.Done:
                        return response;

                    case QueueRetryAction.Drop:
                        throw new QueueRequestFailedException(decision.Reason, response);

                    case QueueRetryAction.Suspend:
                        _log?.Warn($"Immediate request to {request.Endpoint} was refused: " +
                                   $"{decision.Reason ?? "no reason given"}. The immediate lane is " +
                                   "never suspended — the caller decides what to do.");
                        throw new QueueRequestFailedException(decision.Reason, response);
                }

                if (response.TransportFailure)
                {
                    transportFailures++;
                }
                else
                {
                    attempts++;
                }

                if (attempts + transportFailures >= _maxAttempts)
                {
                    throw new QueueRequestFailedException(
                        decision.Reason ?? "retries are exhausted", response);
                }

                TimeSpan delay = decision.Delay ?? _backoff.Next(attempts + transportFailures - 1);
                if (delay > TimeSpan.Zero)
                {
                    // Realtime намеренно: в однопоточной сборке для браузера потоковые таймеры не
                    // срабатывают, а масштаб времени приложения к сроку ответа сервера отношения
                    // не имеет.
                    await UniTask.Delay(delay, DelayType.Realtime, cancellationToken: cancellationToken);
                }
            }
        }
    }
}
