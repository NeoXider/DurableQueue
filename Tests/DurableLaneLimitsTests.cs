using System.Collections.Generic;
using Neoxider.DurableQueue.Lanes;
using NUnit.Framework;

namespace Neoxider.DurableQueue.Tests
{
    /// <summary>
    /// Границы объёма и два класса записей: кем жертвуем при переполнении, кем не жертвуем никогда и
    /// что схлопывается.
    /// </summary>
    public sealed class DurableLaneLimitsTests
    {
        private const string Payload = "0123456789012345678901234567890123456789";

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

        private DurableLane<string> Lane(DurableLaneOptions options)
        {
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage,
                new RecordingDispatcher(),
                new ScriptedPolicy(_ => QueueRetryDecision.Done()),
                _clock,
                _log,
                options);
            lane.Incident += incident => _incidents.Add(incident);
            return lane;
        }

        /// <summary>
        /// Размер одной записи меряется, а не считается в уме: иначе тест придётся переписывать при
        /// каждой правке формата, и однажды его перепишут «чтобы позеленел».
        /// </summary>
        private int MeasureOneRecord()
        {
            FakeQueueStorage probeStorage = new();
            DurableLane<string> probe = QueueTestFactory.Lane(
                probeStorage,
                new RecordingDispatcher(),
                new ScriptedPolicy(_ => QueueRetryDecision.Done()),
                new FakeQueueClock(),
                new FakeQueueLog());
            probe.Enqueue("fact", "cheap", Payload, target: "t");
            return probe.Snapshot().Bytes;
        }

        [Test]
        public void StateCollapsesToTheLastValue_BecauseOnlyTheLastOneMatters()
        {
            DurableLane<string> lane = Lane(QueueTestFactory.Options());

            lane.Enqueue("snapshot", "doc", "v1", QueueRecordClass.State, target: "t");
            lane.Enqueue("snapshot", "doc", "v2", QueueRecordClass.State, target: "t");
            lane.Enqueue("snapshot", "doc", "v3", QueueRecordClass.State, target: "t");

            Assert.AreEqual(1, lane.Count, "глубина потока снимков всегда 1");
            Assert.AreEqual("v3", lane.Records[0].Payload);
        }

        [Test]
        public void StateOfAnotherKind_IsNotCollapsedIntoTheSameSlot()
        {
            DurableLane<string> lane = Lane(QueueTestFactory.Options());

            lane.Enqueue("snapshot-a", "doc", "v1", QueueRecordClass.State, target: "t");
            lane.Enqueue("snapshot-b", "doc", "v1", QueueRecordClass.State, target: "t");

            Assert.AreEqual(2, lane.Count, "это разные снимки, а не два значения одного");
        }

        /// <summary>
        /// Схлопнувшая запись несёт СВОЙ ключ идемпотентности. Унаследовать чужой значило бы однажды
        /// получить от сервера «уже применено» на данные, которых он не видел.
        /// </summary>
        [Test]
        public void CollapsingSnapshot_CarriesItsOwnIdempotencyKey()
        {
            DurableLane<string> lane = Lane(QueueTestFactory.Options());

            lane.Enqueue("snapshot", "doc", "v1", QueueRecordClass.State, "revision-1", "t");
            lane.Enqueue("snapshot", "doc", "v2", QueueRecordClass.State, "revision-2", "t");

            Assert.AreEqual("revision-2", lane.Records[0].IdempotencyKey);
        }

        [Test]
        public void EventsNeverCollapse_BecauseEachOneIsAFactOfItsOwn()
        {
            DurableLane<string> lane = Lane(QueueTestFactory.Options());

            lane.Enqueue("fact", "alpha", "v1", target: "t");
            lane.Enqueue("fact", "alpha", "v2", target: "t");

            Assert.AreEqual(2, lane.Count);
        }

        /// <summary>
        /// Порядок жертв назначает приложение, и он ведётся по цене потери. Библиотека только
        /// исполняет: сперва самый дешёвый поток, внутри потока — самые старые записи.
        /// </summary>
        [Test]
        public void Eviction_TakesTheCheapestStreamFirst()
        {
            int oneRecord = MeasureOneRecord();
            QueueLimits limits = new() { TotalBudgetBytes = oneRecord * 3 + 16 };
            DurableLane<string> lane = Lane(QueueTestFactory.Options(
                new[]
                {
                    new QueueStreamOptions("cheap", true, 1), new QueueStreamOptions("dear", true, 2,
                        QueueEvictionSeverity.Error),
                    new QueueStreamOptions("sacred")
                },
                limits));

            lane.Enqueue("fact", "cheap", Payload, target: "t");
            lane.Enqueue("fact", "cheap", Payload, target: "t");
            lane.Enqueue("fact", "dear", Payload, target: "t");
            lane.Enqueue("fact", "sacred", Payload, target: "t");

            Assert.AreEqual(3, lane.Count);
            int cheapLeft = 0;
            foreach (QueueRecord<string> record in lane.Records)
            {
                if (record.Stream == "cheap")
                {
                    cheapLeft++;
                }
            }

            Assert.AreEqual(1, cheapLeft, "жертвуем самым дешёвым, и по одной записи, а не потоком");
            Assert.AreEqual(1, _incidents.Count);
            Assert.AreEqual(QueueIncidentReason.Evicted, _incidents[0].Reason);
            Assert.AreEqual("cheap", _incidents[0].Stream);
            Assert.IsNotEmpty(_log.Warnings, "вытеснение обязано быть слышно");
        }

        /// <summary>
        /// Неприкосновенный поток не вытесняется НИКОГДА, даже когда места нет совсем: превышение
        /// бюджета — наша внутренняя проблема, потеря чужой работы — уже не наша. Незарегистрированный
        /// поток считается неприкосновенным по умолчанию по той же причине.
        /// </summary>
        [Test]
        public void ProtectedStreams_AreNeverEvicted_TheBudgetGivesWayInstead()
        {
            int oneRecord = MeasureOneRecord();
            QueueLimits limits = new() { TotalBudgetBytes = oneRecord };
            DurableLane<string> lane = Lane(QueueTestFactory.Options(
                new[] { new QueueStreamOptions("sacred") },
                limits));

            lane.Enqueue("fact", "sacred", Payload, target: "t");
            lane.Enqueue("fact", "sacred", Payload, target: "t");
            QueueEnqueueResult third = lane.Enqueue("fact", "unknown-stream", Payload, target: "t");

            Assert.AreEqual(3, lane.Count, "ни одна запись не выброшена");
            Assert.IsTrue(third.Accepted);
            Assert.IsTrue(third.OverBudget, "превышение принято осознанно и названо вслух");
            Assert.IsNotEmpty(_log.Errors);

            bool overBudgetReported = false;
            foreach (QueueIncident incident in _incidents)
            {
                overBudgetReported |= incident.Reason == QueueIncidentReason.OverBudget;
                Assert.AreNotEqual(QueueIncidentReason.Evicted, incident.Reason);
            }

            Assert.IsTrue(overBudgetReported);
        }

        /// <summary>
        /// Запись больше капа не принимается вовсе: молча урезанная нагрузка хуже отказа, потому что
        /// отказ виден сразу, а урезание обнаружится у получателя и без объяснений.
        /// </summary>
        [Test]
        public void RecordOverTheSingleRecordCap_IsRejectedVisibly()
        {
            QueueLimits limits = new() { MaxRecordBytes = 48 };
            DurableLane<string> lane = Lane(QueueTestFactory.Options(limits: limits));

            QueueEnqueueResult result = lane.Enqueue("fact", "alpha", Payload + Payload, target: "t");

            Assert.IsFalse(result.Accepted);
            Assert.AreEqual(QueueEnqueueStatus.RejectedTooLarge, result.Status);
            Assert.AreEqual(0, lane.Count);
            Assert.AreEqual(1, _incidents.Count);
            Assert.AreEqual(QueueIncidentReason.TooLarge, _incidents[0].Reason);
            Assert.IsNotEmpty(_log.Errors);
        }
    }
}
