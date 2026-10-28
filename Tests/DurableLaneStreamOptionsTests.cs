using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Neoxider.DurableQueue.Lanes;
using NUnit.Framework;

namespace Neoxider.DurableQueue.Tests
{
    /// <summary>
    /// Опции, которые приложение назначает потоку фактов, терять которые нельзя никогда: временный
    /// отказ не расходует потолок, срок жизни не применяется, отказ носителя не снимает запись.
    /// </summary>
    public sealed class DurableLaneStreamOptionsTests
    {
        private FakeQueueStorage _storage;
        private FakeQueueClock _clock;
        private FakeQueueLog _log;
        private List<QueueIncident> _incidents;

        [SetUp]
        public void SetUp()
        {
            _storage = new FakeQueueStorage();
            _clock = new FakeQueueClock();
            _log = new FakeQueueLog();
            _incidents = new List<QueueIncident>();
        }

        [Test]
        public async Task FreeRetry_MovesTheBackoffButNeverExhaustsTheCeiling()
        {
            RecordingDispatcher dispatcher = new(batch =>
                QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), 503));
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(_ => QueueRetryDecision.RetryLater(spendsAttempt: false)),
                QueueTestFactory.Options(limits: new QueueLimits { MaxAttempts = 2 }));
            lane.Enqueue("fact", "progress", "1", target: "t");
            List<TimeSpan> waits = new();

            for (int i = 0; i < 5; i++)
            {
                await lane.PumpAsync();
                TimeSpan wait = lane.Records[0].NextAttemptAtUtc.Value - _clock.UtcNow;
                waits.Add(wait);
                _clock.Advance(wait);
            }

            Assert.AreEqual(1, lane.Count, "временный отказ идемпотентного получателя не повод сдаться");
            Assert.AreEqual(0, lane.Records[0].Attempts);
            Assert.AreEqual(5, lane.Records[0].FreeRetries);
            Assert.IsEmpty(_incidents);
            Assert.Greater(waits[4], waits[0], "бэкофф обязан расти и от бесплатных повторов");
        }

        [Test]
        public async Task FreeRetries_SurviveARestart()
        {
            RecordingDispatcher dispatcher = new(batch =>
                QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), 503));
            ScriptedPolicy policy = new(_ => QueueRetryDecision.RetryLater(spendsAttempt: false));
            DurableLane<string> lane = Lane(dispatcher, policy);
            lane.Enqueue("fact", "progress", "1", target: "t");
            await lane.PumpAsync();

            DurableLane<string> restarted = Lane(dispatcher, policy);

            Assert.AreEqual(1, restarted.Records[0].FreeRetries, "иначе перезапуск сбрасывает бэкофф в ноль");
        }

        [Test]
        public async Task NeverExpiringStream_OutlivesTheTtl()
        {
            RecordingDispatcher dispatcher = new();
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(_ => QueueRetryDecision.Done()),
                QueueTestFactory.Options(
                    new[] { new QueueStreamOptions("progress", neverExpires: true) },
                    new QueueLimits { Ttl = TimeSpan.FromDays(7) }));
            lane.Enqueue("fact", "progress", "kept", target: "t");
            lane.Enqueue("fact", "telemetry", "expired", target: "t");
            _clock.Advance(TimeSpan.FromDays(90));

            await lane.PumpAsync();

            Assert.AreEqual(1, dispatcher.Calls, "долгожданная доставка — только у бессрочного потока");
            Assert.AreEqual("kept", dispatcher.Batches[0].Head.Payload);
            Assert.AreEqual(1, _incidents.Count);
            Assert.AreEqual("telemetry", _incidents[0].Stream);
        }

        [Test]
        public async Task RetainingStream_KeepsARecordTheStorageRefused_AndStillDeliversIt()
        {
            _storage.WriteRefuses = true;
            RecordingDispatcher dispatcher = new();
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(_ => QueueRetryDecision.Done()),
                QueueTestFactory.Options(new[] { new QueueStreamOptions("progress", retainWhenStorageRefuses: true) }));

            QueueEnqueueResult result = lane.Enqueue("fact", "progress", "1", target: "t");
            await lane.PumpAsync();

            Assert.AreEqual(QueueEnqueueStatus.AcceptedInMemoryOnly, result.Status);
            Assert.IsTrue(result.Accepted);
            Assert.IsFalse(result.IsPersisted);
            Assert.AreEqual(1, dispatcher.Calls, "запись доставляется в этом запуске, несмотря на отказ носителя");
            Assert.IsNotEmpty(_log.Errors, "о жизни только в памяти молчать нельзя");
        }

        [Test]
        public void OrdinaryStream_StillRejectsWhatTheStorageRefused()
        {
            _storage.WriteRefuses = true;
            DurableLane<string> lane = Lane(new RecordingDispatcher(), new ScriptedPolicy(_ => QueueRetryDecision.Done()));

            QueueEnqueueResult result = lane.Enqueue("fact", "telemetry", "1", target: "t");

            Assert.AreEqual(QueueEnqueueStatus.RejectedStorageFailure, result.Status);
            Assert.AreEqual(0, lane.Count);
        }

        [Test]
        public async Task MaxBatchSize_LimitsHowManyRecordsGoInOneBatch()
        {
            RecordingDispatcher dispatcher = new();
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(_ => QueueRetryDecision.Done()),
                QueueTestFactory.Options(new[] { new QueueStreamOptions("telemetry", maxBatchSize: 2) }));
            for (int i = 0; i < 5; i++)
            {
                lane.Enqueue("event", "telemetry", i.ToString(), target: "t");
            }

            await lane.PumpAsync();

            CollectionAssert.AreEqual(new[] { 2, 2, 1 }, dispatcher.Batches.ConvertAll(b => b.Records.Count));
            Assert.AreEqual(0, lane.Count);
        }

        [Test]
        public async Task RejectedRecord_LeavesADeadLetterRow_WhileTheRestOfTheBatchIsDelivered()
        {
            RecordingDispatcher dispatcher = new(batch =>
            {
                batch.Reject(batch.Records[1], 422, "malformed");
                return QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), 200);
            });
            DurableLane<string> lane = Lane(dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.Done()));
            lane.Enqueue("event", "telemetry", "good", target: "t", idempotencyKey: "a");
            lane.Enqueue("event", "telemetry", "poison", target: "t", idempotencyKey: "b");
            lane.Enqueue("event", "telemetry", "good too", target: "t", idempotencyKey: "c");

            await lane.PumpAsync();

            Assert.AreEqual(0, lane.Count);
            QueueDeadLetterEntry entry = lane.Snapshot().DeadLetter[0];
            Assert.AreEqual(1, lane.Snapshot().DeadLetter.Count);
            Assert.AreEqual("b", entry.IdempotencyKey);
            Assert.AreEqual(422, entry.StatusCode);
            Assert.AreEqual(QueueIncidentReason.PermanentRejection, _incidents[0].Reason);
        }

        [Test]
        public async Task RejectedRecord_IsRemovedEvenWhenTheRestIsRetried()
        {
            RecordingDispatcher dispatcher = new(batch =>
            {
                batch.Reject(batch.Records[0], 422, "malformed");
                return QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), 503);
            });
            DurableLane<string> lane = Lane(dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.RetryLater()));
            lane.Enqueue("event", "telemetry", "poison", target: "t", idempotencyKey: "a");
            lane.Enqueue("event", "telemetry", "waits", target: "t", idempotencyKey: "b");

            await lane.PumpAsync();

            Assert.AreEqual(1, lane.Count);
            Assert.AreEqual("waits", lane.Records[0].Payload);
            Assert.AreEqual(1, lane.Records[0].Attempts, "исход пачки применён к тому, что осталось");
            Assert.AreEqual(1, lane.Snapshot().DeadLetter.Count);
        }

        [Test]
        public async Task ConfirmedRecords_LeaveTheQueue_EvenWhenTheRestIsRetried()
        {
            RecordingDispatcher dispatcher = new(batch =>
            {
                batch.Confirm(batch.Records[0]);
                return QueueResponse.FromTransportFailure(RecordingDispatcher.Endpoint(batch), "cut");
            });
            DurableLane<string> lane = Lane(dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.RetryLater()));
            lane.Enqueue("event", "telemetry", "delivered", target: "t");
            lane.Enqueue("event", "telemetry", "waits", target: "t");

            await lane.PumpAsync();

            Assert.AreEqual(1, lane.Count, "подтверждённая часть снята сразу");
            Assert.AreEqual("waits", lane.Records[0].Payload);
            Assert.AreEqual(1, lane.Records[0].TransportFailures);
            Assert.IsEmpty(lane.Snapshot().DeadLetter, "подтверждение следа в dead-letter не оставляет");
        }

        [Test]
        public async Task IndependentTargets_AStuckTargetDoesNotHoldTheOthers()
        {
            RecordingDispatcher dispatcher = new(batch =>
                QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), batch.Target == "a" ? 503 : 200));
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(a => a.Response.StatusCode == 200
                    ? QueueRetryDecision.Done()
                    : QueueRetryDecision.RetryLater(spendsAttempt: false)),
                QueueTestFactory.Options(new[] { new QueueStreamOptions("progress", independentTargets: true) }));
            lane.Enqueue("fact", "progress", "a1", target: "a");
            lane.Enqueue("fact", "progress", "a2", target: "a");
            lane.Enqueue("fact", "progress", "b1", target: "b");

            await lane.PumpAsync();
            await lane.PumpAsync();

            CollectionAssert.AreEqual(new[] { "a1", "a2" }, lane.Records.Select(r => r.Payload).ToArray(),
                "застрявший адрес держит только себя, порядок внутри адреса цел");
            Assert.IsFalse(dispatcher.Batches.Exists(b => b.Head.Payload == "a2"), "a2 не обгоняет a1");
            Assert.IsTrue(dispatcher.Batches.Exists(b => b.Target == "b"), "соседний адрес доставлен в том же проходе");
        }

        [Test]
        public async Task OrdinaryStream_AStuckHeadHoldsTheWholeStream()
        {
            RecordingDispatcher dispatcher = new(batch =>
                QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), batch.Target == "a" ? 503 : 200));
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(a => a.Response.StatusCode == 200
                    ? QueueRetryDecision.Done()
                    : QueueRetryDecision.RetryLater()));
            lane.Enqueue("fact", "progress", "a1", target: "a");
            lane.Enqueue("fact", "progress", "b1", target: "b");

            await lane.PumpAsync();

            Assert.AreEqual(2, lane.Count, "без опции поток — строгий FIFO");
            Assert.AreEqual(1, dispatcher.Calls);
        }

        [Test]
        public async Task UnknownKind_IsKeptButNeverBlocksTheStream()
        {
            RecordingDispatcher dispatcher = new();
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage,
                dispatcher,
                new ScriptedPolicy(_ => QueueRetryDecision.Done()),
                _clock,
                _log,
                new DurableLaneOptions(QueueTestFactory.Key, knownKinds: new[] { "fact" }));
            lane.Enqueue("from_the_future", "progress", "later", target: "t");
            lane.Enqueue("fact", "progress", "now", target: "t");

            await lane.PumpAsync();

            Assert.AreEqual(1, dispatcher.Calls);
            Assert.AreEqual("now", dispatcher.Batches[0].Head.Payload);
            Assert.AreEqual("later", lane.Records.Single().Payload, "незнакомая запись хранится для версии, которая её знает");
            Assert.AreEqual(1, _log.Warnings.Count, "о незнакомом виде сказано один раз");
        }

        private DurableLane<string> Lane(
            RecordingDispatcher dispatcher,
            ScriptedPolicy policy,
            DurableLaneOptions options = null)
        {
            DurableLane<string> lane = QueueTestFactory.Lane(_storage, dispatcher, policy, _clock, _log, options);
            lane.Incident += incident => _incidents.Add(incident);
            return lane;
        }
    }
}
