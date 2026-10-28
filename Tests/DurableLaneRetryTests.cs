using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Neoxider.DurableQueue.Lanes;
using NUnit.Framework;

namespace Neoxider.DurableQueue.Tests
{
    /// <summary>
    /// Повторы: чем считается неудача, что двигает бэкофф, что расходует потолок и куда девается
    /// неповторяемое.
    /// </summary>
    public sealed class DurableLaneRetryTests
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

        private DurableLane<string> Lane(
            RecordingDispatcher dispatcher,
            ScriptedPolicy policy,
            DurableLaneOptions options = null)
        {
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage, dispatcher, policy, _clock, _log, options);
            lane.Incident += incident => _incidents.Add(incident);
            return lane;
        }

        private static RecordingDispatcher Answering(int statusCode)
        {
            return new RecordingDispatcher(batch =>
                QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), statusCode));
        }

        private static RecordingDispatcher Silent()
        {
            return new RecordingDispatcher(batch =>
                QueueResponse.FromTransportFailure(RecordingDispatcher.Endpoint(batch), "no route"));
        }

        [Test]
        public async Task Done_RemovesRecordFromQueue()
        {
            RecordingDispatcher dispatcher = Answering(200);
            DurableLane<string> lane = Lane(dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.Done()));

            lane.Enqueue("fact", "alpha", "1", target: "t");
            await lane.PumpAsync();

            Assert.AreEqual(0, lane.Count);
            Assert.IsEmpty(_incidents);
        }

        [Test]
        public async Task RetryLater_KeepsRecordAndHonoursTheDelayTheServerNamed()
        {
            RecordingDispatcher dispatcher = Answering(429);
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(_ => QueueRetryDecision.RetryLater(TimeSpan.FromMinutes(3))));

            lane.Enqueue("fact", "alpha", "1", target: "t");
            await lane.PumpAsync();

            Assert.AreEqual(1, lane.Count);
            Assert.AreEqual(1, lane.Records[0].Attempts);
            Assert.AreEqual(
                _clock.UtcNow + TimeSpan.FromMinutes(3),
                lane.Records[0].NextAttemptAtUtc,
                "названный сервером срок важнее нашей арифметики");
        }

        /// <summary>
        /// Постоянный отказ снимает запись, но НЕ молча: и след в журнале недоставленного, и
        /// происшествие для приложения. Молчаливое снятие здесь и есть потеря данных.
        /// </summary>
        [Test]
        public async Task Drop_RemovesRecordButLeavesATrace()
        {
            RecordingDispatcher dispatcher = Answering(409);
            DurableLane<string> lane = Lane(
                dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.Drop("quota is spent")));

            lane.Enqueue("fact", "alpha", "1", target: "t", idempotencyKey: "op-1");
            await lane.PumpAsync();

            Assert.AreEqual(0, lane.Count);
            Assert.AreEqual(1, _incidents.Count);
            Assert.AreEqual(QueueIncidentReason.PermanentRejection, _incidents[0].Reason);
            Assert.AreEqual("quota is spent", _incidents[0].Detail);

            QueueSnapshot snapshot = lane.Snapshot();
            Assert.AreEqual(1, snapshot.DeadLetter.Count);
            Assert.AreEqual("op-1", snapshot.DeadLetter[0].IdempotencyKey);
            Assert.IsNotEmpty(_log.Errors);
        }

        [Test]
        public async Task Suspend_StopsThePumpWithoutSpendingAnAttempt()
        {
            RecordingDispatcher dispatcher = Answering(401);
            DurableLane<string> lane = Lane(
                dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.Suspend("no session")));

            lane.Enqueue("fact", "alpha", "1", target: "t");
            lane.Enqueue("fact", "beta", "2", target: "t");
            await lane.PumpAsync();

            Assert.IsTrue(lane.IsSuspended);
            Assert.AreEqual(2, lane.Count, "приостановка ничего не выбрасывает");
            Assert.AreEqual(0, lane.Records[0].Attempts, "попытка не расходуется");
            Assert.AreEqual(1, dispatcher.Calls, "второй поток даже не пробуется");

            await lane.PumpAsync();
            Assert.AreEqual(1, dispatcher.Calls, "приостановленная полоса молчит");
        }

        [Test]
        public async Task Suspension_IsLiftedByResume()
        {
            RecordingDispatcher dispatcher = new(batch =>
                QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), 200));
            bool suspend = true;
            ScriptedPolicy policy = new(_ =>
                suspend ? QueueRetryDecision.Suspend("no session") : QueueRetryDecision.Done());
            DurableLane<string> lane = Lane(dispatcher, policy);

            lane.Enqueue("fact", "alpha", "1", target: "t");
            await lane.PumpAsync();
            Assert.AreEqual(1, lane.Count);

            suspend = false;
            lane.Resume();
            await lane.PumpAsync();

            Assert.IsFalse(lane.IsSuspended);
            Assert.AreEqual(0, lane.Count, "после возобновления насос начинает без задержки");
        }

        /// <summary>
        /// Возобновление может и не прийти: тот, кто обязан его позвать, ошибается или не доживает.
        /// Приостановленную навсегда очередь снаружи не отличить от пустой, поэтому у приостановки
        /// есть потолок по времени и пробная отправка после него.
        /// </summary>
        [Test]
        public async Task Suspension_IsAlsoLiftedByItsTimeCeiling_WhenNobodyCallsResume()
        {
            RecordingDispatcher dispatcher = Answering(200);
            bool suspend = true;
            ScriptedPolicy policy = new(_ =>
                suspend ? QueueRetryDecision.Suspend("no session") : QueueRetryDecision.Done());

            QueueLimits limits = new() { SuspendCeiling = TimeSpan.FromMinutes(10) };
            DurableLane<string> lane = Lane(dispatcher, policy, QueueTestFactory.Options(limits: limits));

            lane.Enqueue("fact", "alpha", "1", target: "t");
            await lane.PumpAsync();
            Assert.AreEqual(1, dispatcher.Calls);

            _clock.Advance(TimeSpan.FromMinutes(9));
            await lane.PumpAsync();
            Assert.AreEqual(1, dispatcher.Calls, "потолок ещё не вышел");

            suspend = false;
            _clock.Advance(TimeSpan.FromMinutes(2));
            await lane.PumpAsync();

            Assert.AreEqual(2, dispatcher.Calls, "после потолка полагается пробная отправка");
            Assert.AreEqual(0, lane.Count);
        }

        /// <summary>
        /// Отмена вызывающим — не ошибка доставки. Считать её неудачей значит расходовать попытки на
        /// закрытие приложения и однажды увести данные в недоставленное просто потому, что игру
        /// закрывали.
        /// </summary>
        [Test]
        public async Task CallerCancellation_IsNotAFailureAndSpendsNothing()
        {
            RecordingDispatcher dispatcher = Answering(200);
            dispatcher.ThrowsCancellation = true;
            DurableLane<string> lane = Lane(dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.Done()));

            lane.Enqueue("fact", "alpha", "1", target: "t");
            await lane.PumpAsync();

            Assert.AreEqual(1, lane.Count);
            Assert.AreEqual(0, lane.Records[0].Attempts);
            Assert.AreEqual(0, lane.Records[0].TransportFailures);
            Assert.IsNull(lane.Records[0].NextAttemptAtUtc);
            Assert.IsEmpty(_incidents);
        }

        /// <summary>
        /// Час без сети не имеет права исчерпать попытки: сервер ничего не сказал, значит и судить
        /// не о чем. Бэкофф при этом двигаться обязан — иначе очередь долбит мёртвую сеть.
        /// </summary>
        [Test]
        public async Task TransportFailure_MovesBackoffButNeverSpendsTheAttemptCeiling()
        {
            RecordingDispatcher dispatcher = Silent();
            QueueLimits limits = new() { MaxAttempts = 2 };
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(_ => QueueRetryDecision.RetryLater()),
                QueueTestFactory.Options(limits: limits));

            lane.Enqueue("fact", "alpha", "1", target: "t");

            for (int i = 0; i < 5; i++)
            {
                await lane.PumpAsync();
                _clock.Advance(TimeSpan.FromHours(1));
            }

            Assert.AreEqual(1, lane.Count, "запись цела");
            Assert.AreEqual(0, lane.Records[0].Attempts);
            Assert.AreEqual(5, lane.Records[0].TransportFailures);
            Assert.IsEmpty(_incidents, "недоставленным это не считается");
        }

        [Test]
        public async Task ServerAnswers_SpendTheCeilingAndSendTheRecordToDeadLetter()
        {
            RecordingDispatcher dispatcher = Answering(500);
            QueueLimits limits = new() { MaxAttempts = 2 };
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(_ => QueueRetryDecision.RetryLater()),
                QueueTestFactory.Options(limits: limits));

            lane.Enqueue("fact", "alpha", "1", target: "t");

            for (int i = 0; i < 2; i++)
            {
                await lane.PumpAsync();
                _clock.Advance(TimeSpan.FromHours(1));
            }

            Assert.AreEqual(0, lane.Count);
            Assert.AreEqual(1, _incidents.Count);
            Assert.AreEqual(QueueIncidentReason.AttemptsExhausted, _incidents[0].Reason);
            Assert.AreEqual(1, lane.Snapshot().DeadLetter.Count);
        }

        /// <summary>
        /// Растёт ОКНО задержки, а не каждая отдельная задержка: точка внутри окна случайна, иначе
        /// после общего обрыва связи все клиенты повторяют залпом.
        /// </summary>
        [Test]
        public async Task BackoffWindow_GrowsWithEveryFailure_AndStopsAtTheCap()
        {
            RecordingDispatcher dispatcher = Answering(500);
            DurableLane<string> lane = Lane(
                dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.RetryLater()));

            lane.Enqueue("fact", "alpha", "1", target: "t");

            List<TimeSpan> delays = new();
            for (int i = 0; i < 3; i++)
            {
                DateTime before = _clock.UtcNow;
                await lane.PumpAsync();
                delays.Add(lane.Records[0].NextAttemptAtUtc.Value - before);
                _clock.Advance(TimeSpan.FromHours(1));
            }

            Assert.Less(delays[0], delays[1]);
            Assert.Less(delays[1], delays[2]);

            QueueBackoff backoff = new(random: new MaxJitterRandom());
            Assert.AreEqual(backoff.Cap, backoff.Next(50), "окно упирается в потолок и не растёт дальше");
        }

        /// <summary>
        /// Момент следующей попытки лежит В ЗАПИСИ. Иначе перезапуск сбрасывает бэкофф в ноль, и
        /// очередь, накопившаяся за офлайн, уходит на сервер штормом ровно в момент старта.
        /// </summary>
        [Test]
        public async Task NextAttempt_SurvivesRestart_BecauseItLivesInTheRecord()
        {
            RecordingDispatcher dispatcher = Answering(500);
            DurableLane<string> lane = Lane(
                dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.RetryLater()));

            lane.Enqueue("fact", "alpha", "1", target: "t");
            await lane.PumpAsync();
            DateTime scheduled = lane.Records[0].NextAttemptAtUtc.Value;

            RecordingDispatcher afterRestart = Answering(500);
            DurableLane<string> reloaded = Lane(
                afterRestart, new ScriptedPolicy(_ => QueueRetryDecision.RetryLater()));

            Assert.AreEqual(scheduled, reloaded.Records[0].NextAttemptAtUtc);

            await reloaded.PumpAsync();
            Assert.AreEqual(0, afterRestart.Calls, "перезапуск не отменяет бэкофф");

            _clock.UtcNow = scheduled + TimeSpan.FromSeconds(1);
            await reloaded.PumpAsync();
            Assert.AreEqual(1, afterRestart.Calls);
        }

        /// <summary>
        /// Диспетчер обязан превращать свой отказ в ответ сам. Если он этого не сделал, насос не
        /// падает и не молчит: считает случившееся отсутствием ответа (потолок не расходуется) и
        /// пишет ошибку — иначе дефект диспетчера годами выглядит как «сеть шалит».
        /// </summary>
        [Test]
        public async Task DispatcherThatThrows_IsTreatedAsSilenceAndIsLoggedAsAnError()
        {
            RecordingDispatcher dispatcher = Answering(200);
            dispatcher.Throws = new InvalidOperationException("client is broken");
            DurableLane<string> lane = Lane(
                dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.RetryLater()));

            lane.Enqueue("fact", "alpha", "1", target: "t");
            await lane.PumpAsync();

            Assert.AreEqual(1, lane.Count);
            Assert.AreEqual(0, lane.Records[0].Attempts);
            Assert.AreEqual(1, lane.Records[0].TransportFailures);
            Assert.IsNotEmpty(_log.Errors);
        }

        /// <summary>
        /// Протухший СНИМОК хуже отсутствующего: доехав, он перезапишет более свежее состояние.
        /// Поэтому его просто нет — и в недоставленное он не попадает.
        /// </summary>
        [Test]
        public async Task ExpiredState_IsDroppedAndDoesNotReachDeadLetter()
        {
            RecordingDispatcher dispatcher = Answering(200);
            QueueLimits limits = new() { Ttl = TimeSpan.FromDays(7) };
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(_ => QueueRetryDecision.Done()),
                QueueTestFactory.Options(limits: limits));

            lane.Enqueue("snapshot", "alpha", "1", QueueRecordClass.State, target: "t");
            _clock.Advance(TimeSpan.FromDays(8));
            await lane.PumpAsync();

            Assert.AreEqual(0, lane.Count);
            Assert.AreEqual(0, dispatcher.Calls, "протухший снимок отправлять нельзя");
            Assert.AreEqual(0, lane.Snapshot().DeadLetter.Count);
            Assert.AreEqual(1, _incidents.Count);
            Assert.AreEqual(QueueIncidentReason.Expired, _incidents[0].Reason);
            Assert.IsNotEmpty(_log.Warnings);
        }

        /// <summary>
        /// Протухший ФАКТ — это потерянный факт. Молча удалять недоставленное по таймеру — то же
        /// самое, от чего очередь и защищает, поэтому он уходит в недоставленное со следом.
        /// </summary>
        [Test]
        public async Task ExpiredEvent_GoesToDeadLetterInsteadOfVanishing()
        {
            RecordingDispatcher dispatcher = Answering(200);
            QueueLimits limits = new() { Ttl = TimeSpan.FromDays(7) };
            DurableLane<string> lane = Lane(
                dispatcher,
                new ScriptedPolicy(_ => QueueRetryDecision.Done()),
                QueueTestFactory.Options(limits: limits));

            lane.Enqueue("fact", "alpha", "1", QueueRecordClass.Event, "op-1", "t");
            _clock.Advance(TimeSpan.FromDays(8));
            await lane.PumpAsync();

            Assert.AreEqual(0, lane.Count);
            Assert.AreEqual(0, dispatcher.Calls);
            Assert.AreEqual(1, lane.Snapshot().DeadLetter.Count);
            Assert.AreEqual("expired", lane.Snapshot().DeadLetter[0].Reason);
            Assert.AreEqual(QueueIncidentReason.Expired, _incidents[0].Reason);
        }
    }
}
