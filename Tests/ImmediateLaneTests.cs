using System;
using System.Threading.Tasks;
using Neoxider.DurableQueue.Lanes;
using NUnit.Framework;

namespace Neoxider.DurableQueue.Tests
{
    /// <summary>
    /// Немедленная полоса. Проверяем одно свойство, ради которого она и отделена от долговечной:
    /// отказ здесь ВИДЕН вызывающему.
    /// </summary>
    public sealed class ImmediateLaneTests
    {
        private static readonly QueueRequest Request = new("GET", "/catalog", "/catalog");

        private FakeQueueLog _log;

        [SetUp]
        public void SetUp() => _log = new FakeQueueLog();

        private static QueueResponse Answer(int statusCode) =>
            QueueResponse.FromServer(new QueueEndpoint("GET", "/catalog"), statusCode);

        private ImmediateLane Lane(RecordingTransport transport, ScriptedPolicy policy) =>
            new(transport, policy, new QueueBackoff(random: new MaxJitterRandom()), _log, 3);

        [Test]
        public async Task Answer_IsReturnedToTheCaller()
        {
            RecordingTransport transport = new(_ => Answer(200));
            ImmediateLane lane = Lane(transport, new ScriptedPolicy(_ => QueueRetryDecision.Done()));

            QueueResponse response = await lane.SendAsync(Request);

            Assert.AreEqual(200, response.StatusCode);
            Assert.AreEqual(1, transport.Calls);
        }

        [Test]
        public async Task RetryInsideOneCall_IsAllowedAndTheAnswerStillComesBack()
        {
            RecordingTransport transport = new(call => Answer(call == 0 ? 503 : 200));
            ScriptedPolicy policy = new(attempt => attempt.Response.StatusCode == 200
                ? QueueRetryDecision.Done()
                : QueueRetryDecision.RetryLater(TimeSpan.Zero));
            ImmediateLane lane = Lane(transport, policy);

            QueueResponse response = await lane.SendAsync(Request);

            Assert.AreEqual(200, response.StatusCode);
            Assert.AreEqual(2, transport.Calls);
        }

        /// <summary>
        /// Отдать вызывающему пустоту вместо ошибки — это и есть тот молчаливый отказ, из-за которого
        /// проблемы находят через недели, а не через секунду.
        /// </summary>
        [Test]
        public void PermanentRejection_ThrowsInsteadOfReturningEmptiness()
        {
            RecordingTransport transport = new(_ => Answer(422));
            ImmediateLane lane = Lane(
                transport, new ScriptedPolicy(_ => QueueRetryDecision.Drop("the name is not allowed")));

            QueueRequestFailedException failure = Assert.ThrowsAsync<QueueRequestFailedException>(async () => await lane.SendAsync(Request));

            Assert.AreEqual("the name is not allowed", failure.Reason);
            Assert.AreEqual(422, failure.StatusCode);
            Assert.AreEqual(1, transport.Calls, "постоянный отказ повтором не лечится");
        }

        [Test]
        public void ExhaustedRetries_ThrowToo_TheCallerIsStillWaiting()
        {
            RecordingTransport transport = new(_ => Answer(503));
            ImmediateLane lane = Lane(
                transport, new ScriptedPolicy(_ => QueueRetryDecision.RetryLater(TimeSpan.Zero)));

            Assert.ThrowsAsync<QueueRequestFailedException>(async () => await lane.SendAsync(Request));
            Assert.AreEqual(3, transport.Calls);
        }

        /// <summary>
        /// Приостановка немедленной полосе не назначается никогда: через неё идёт в том числе
        /// восстановление доступа, то есть ровно то, чем приостановка снимается. Запрети её здесь —
        /// и снять приостановку станет нечем.
        /// </summary>
        [Test]
        public void SuspendRequest_BecomesAVisibleErrorInsteadOfSilence()
        {
            RecordingTransport transport = new(_ => Answer(401));
            ImmediateLane lane = Lane(
                transport, new ScriptedPolicy(_ => QueueRetryDecision.Suspend("no session")));

            QueueRequestFailedException failure = Assert.ThrowsAsync<QueueRequestFailedException>(async () => await lane.SendAsync(Request));

            Assert.AreEqual(401, failure.StatusCode);
            Assert.AreEqual(1, transport.Calls, "полоса не замолкает, а отвечает отказом");
            Assert.IsNotEmpty(_log.Warnings);
        }
    }
}
