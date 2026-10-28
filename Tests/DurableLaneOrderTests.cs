using System.Threading.Tasks;
using Neoxider.DurableQueue.Lanes;
using NUnit.Framework;

namespace Neoxider.DurableQueue.Tests
{
    /// <summary>
    /// Порядок и адресация: что очередь обещает про последовательность доставки и про то, КУДА
    /// доставляет.
    /// </summary>
    public sealed class DurableLaneOrderTests
    {
        private FakeQueueStorage _storage;
        private FakeQueueClock _clock;
        private FakeQueueLog _log;

        [SetUp]
        public void SetUp()
        {
            _storage = new FakeQueueStorage();
            _clock = new FakeQueueClock();
            _log = new FakeQueueLog();
        }

        private static ScriptedPolicy AlwaysDone() => new(_ => QueueRetryDecision.Done());

        private static ScriptedPolicy AlwaysRetry() =>
            new(_ => QueueRetryDecision.RetryLater(reason: "server is unwell"));

        [Test]
        public async Task Fifo_InsideStream_DeliversInEnqueueOrder()
        {
            RecordingDispatcher dispatcher = new();
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage, dispatcher, AlwaysDone(), _clock, _log);

            lane.Enqueue("first", "alpha", "1", target: "t");
            lane.Enqueue("second", "alpha", "2", target: "t");
            lane.Enqueue("third", "alpha", "3", target: "t");

            await lane.PumpAsync();

            Assert.AreEqual(3, dispatcher.Calls);
            Assert.AreEqual("first", dispatcher.Batches[0].Kind);
            Assert.AreEqual("second", dispatcher.Batches[1].Kind);
            Assert.AreEqual("third", dispatcher.Batches[2].Kind);
            Assert.AreEqual(0, lane.Count);
        }

        [Test]
        public async Task LeadingRunOfTheSameKindAndTarget_TravelsAsOneBatch()
        {
            RecordingDispatcher dispatcher = new();
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage, dispatcher, AlwaysDone(), _clock, _log);

            lane.Enqueue("message", "alpha", "1", target: "room-1");
            lane.Enqueue("message", "alpha", "2", target: "room-1");
            lane.Enqueue("message", "alpha", "3", target: "room-2");

            await lane.PumpAsync();

            Assert.AreEqual(2, dispatcher.Calls, "отрезок обязан оборваться на смене адреса");
            Assert.AreEqual(2, dispatcher.Batches[0].Records.Count);
            Assert.AreEqual("room-1", dispatcher.Batches[0].Target);
            Assert.AreEqual(1, dispatcher.Batches[1].Records.Count);
            Assert.AreEqual("room-2", dispatcher.Batches[1].Target);
        }

        [Test]
        public async Task Streams_AreIndependent_AndBothDrainInOnePass()
        {
            RecordingDispatcher dispatcher = new();
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage, dispatcher, AlwaysDone(), _clock, _log);

            lane.Enqueue("a", "alpha", "1", target: "t");
            lane.Enqueue("b", "beta", "2", target: "t");

            await lane.PumpAsync();

            Assert.AreEqual(0, lane.Count);
            Assert.AreEqual(2, dispatcher.Calls);
        }

        /// <summary>
        /// Ядовитая запись держит СВОЙ поток и только его. Глобальный порядок мы не обещаем именно
        /// поэтому: одна вечно падающая запись иначе останавливает и то, терять что нельзя.
        /// </summary>
        [Test]
        public async Task PoisonRecord_HoldsItsOwnStreamOnly()
        {
            RecordingDispatcher dispatcher = new(batch => batch.Stream == "poisoned"
                ? QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), 500)
                : QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), 200));

            ScriptedPolicy policy = new(attempt => attempt.Response.StatusCode == 200
                ? QueueRetryDecision.Done()
                : QueueRetryDecision.RetryLater());

            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage, dispatcher, policy, _clock, _log);

            lane.Enqueue("a", "poisoned", "1", target: "t");
            lane.Enqueue("a", "poisoned", "2", target: "t");
            lane.Enqueue("b", "healthy", "3", target: "t");
            lane.Enqueue("b", "healthy", "4", target: "t");

            await lane.PumpAsync();

            Assert.AreEqual(2, lane.Count, "здоровый поток уехал целиком");
            foreach (QueueRecord<string> record in lane.Records)
            {
                Assert.AreEqual("poisoned", record.Stream);
            }
        }

        /// <summary>
        /// Адрес берётся из момента ПОСТАНОВКИ. Переспросить его при отправке библиотеке нечем и
        /// незачем: пока адрес разрешался в момент отправки, он успевал смениться, и хвост записей
        /// уезжал не туда, куда его писали.
        /// </summary>
        [Test]
        public async Task Target_IsTakenFromEnqueueMoment_NotFromSendMoment()
        {
            RecordingDispatcher dispatcher = new();
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage, dispatcher, AlwaysDone(), _clock, _log);

            lane.Enqueue("message", "alpha", "written for the first target", target: "first");
            lane.Enqueue("message", "alpha", "written for the second target", target: "second");

            await lane.PumpAsync();

            Assert.AreEqual("first", dispatcher.Batches[0].Target);
            Assert.AreEqual("written for the first target", dispatcher.Batches[0].Head.Payload);
            Assert.AreEqual("second", dispatcher.Batches[1].Target);
        }

        [Test]
        public async Task RecordWithoutTarget_IsNotSentAndSpendsNoAttempt()
        {
            RecordingDispatcher dispatcher = new();
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage, dispatcher, AlwaysDone(), _clock, _log);

            lane.Enqueue("message", "alpha", "1");

            await lane.PumpAsync();

            Assert.AreEqual(0, dispatcher.Calls, "слать некуда — это не отправка");
            Assert.AreEqual(1, lane.Count);
            Assert.AreEqual(0, lane.Records[0].Attempts);
            Assert.AreEqual(0, lane.Records[0].TransportFailures);
            Assert.IsNull(lane.Records[0].NextAttemptAtUtc, "бэкофф на «адреса ещё нет» не тратится");
        }

        /// <summary>
        /// Адрес обязан лечь на носитель ДО сетевого вызова: упавшая отправка иначе вернёт записи в
        /// очередь снова безадресными, и следующая смена адреса увезёт их в чужое место.
        /// </summary>
        [Test]
        public async Task AssignTargetToUnaddressed_ReachesStorageBeforeAnySend()
        {
            RecordingDispatcher dispatcher = new(batch =>
                QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), 500));
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage, dispatcher, AlwaysRetry(), _clock, _log);

            lane.Enqueue("message", "alpha", "1");
            lane.Enqueue("message", "beta", "2");

            int flushesBefore = _storage.FlushCount;
            int assigned = lane.AssignTargetToUnaddressed("alpha", "resolved");

            Assert.AreEqual(1, assigned, "чужой поток адрес не получает");
            Assert.Greater(_storage.FlushCount, flushesBefore, "адрес обязан дойти до носителя сразу");
            StringAssert.Contains("resolved", _storage.Raw(QueueTestFactory.Key));

            await lane.PumpAsync();

            DurableLane<string> reloaded = QueueTestFactory.Lane(
                _storage, new RecordingDispatcher(), AlwaysDone(), _clock, _log);
            Assert.AreEqual("resolved", reloaded.Records[0].Target,
                "упавшая отправка не имеет права вернуть запись безадресной");
        }

        [Test]
        public void Readdress_MovesOnlyRecordsOfThatStreamAndTarget()
        {
            RecordingDispatcher dispatcher = new();
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage, dispatcher, AlwaysDone(), _clock, _log);

            lane.Enqueue("message", "alpha", "1", target: "gone");
            lane.Enqueue("message", "alpha", "2", target: "alive");
            lane.Enqueue("message", "beta", "3", target: "gone");

            int moved = lane.Readdress("alpha", "gone", "fresh");

            Assert.AreEqual(1, moved);
            Assert.AreEqual("fresh", lane.Records[0].Target);
            Assert.AreEqual("alive", lane.Records[1].Target);
            Assert.AreEqual("gone", lane.Records[2].Target, "чужой поток не трогаем");
            Assert.IsNotEmpty(_log.Warnings, "перенос адреса обязан быть слышен");
        }
    }
}
