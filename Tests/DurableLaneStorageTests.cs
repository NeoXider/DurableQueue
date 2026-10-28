using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Neoxider.DurableQueue.Lanes;
using Neoxider.DurableQueue.Ports;
using NUnit.Framework;

namespace Neoxider.DurableQueue.Tests
{
    /// <summary>
    /// Хранение: отложенное чтение, отказ носителя, битое содержимое и подменённый формат.
    /// Каждый из этих тестов существует из-за реального дефекта, а не из любви к полноте.
    /// </summary>
    public sealed class DurableLaneStorageTests
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

        private DurableLane<string> Lane(
            RecordingDispatcher dispatcher = null, IQueueCodec<string> codec = null)
        {
            return QueueTestFactory.Lane(
                _storage,
                dispatcher ?? new RecordingDispatcher(),
                new ScriptedPolicy(_ => QueueRetryDecision.Done()),
                _clock,
                _log,
                codec: codec);
        }

        /// <summary>
        /// Пока ключ неадресуем, очередь НИЧЕГО не считает прочитанным. Прочитать в конструкторе —
        /// значит получить честное «данных нет» и первой же записью затереть всё сохранённое; ровно
        /// так однажды пропала накопленная история.
        /// </summary>
        [Test]
        public async Task WhileTheKeyIsNotAddressable_NothingIsConsideredRead()
        {
            DurableLane<string> first = Lane();
            first.Enqueue("fact", "alpha", "written earlier", target: "t");
            string savedEarlier = _storage.Raw(QueueTestFactory.Key);
            Assert.IsNotNull(savedEarlier);

            _storage.Addressable = false;
            RecordingDispatcher dispatcher = new();
            DurableLane<string> blind = Lane(dispatcher);

            Assert.AreEqual(0, blind.Count, "чужой пустоты мы не видели — мы вообще ещё не читали");
            blind.Enqueue("fact", "alpha", "written blindly", target: "t");
            Assert.AreEqual(savedEarlier, _storage.Raw(QueueTestFactory.Key),
                "вслепую писать поверх сохранённого нельзя");

            await blind.PumpAsync();
            Assert.AreEqual(0, dispatcher.Calls,
                "отправлять раньше чтения — значит переставить поставленное вперёд непрочитанного");

            _storage.Addressable = true;

            Assert.AreEqual(2, blind.Count, "дочитали старое и не потеряли поставленное вслепую");
            Assert.AreEqual("written earlier", blind.Records[0].Payload);
            Assert.AreEqual("written blindly", blind.Records[1].Payload);
            StringAssert.Contains("written earlier", _storage.Raw(QueueTestFactory.Key));
            StringAssert.Contains("written blindly", _storage.Raw(QueueTestFactory.Key));
        }

        /// <summary>
        /// Отказ хранилища превращается в отказ ПОСТАНОВКИ, а не в исключение: постановка живёт в
        /// кадре обычной работы приложения, и вызывающий, который «просто записал факт», к
        /// исключению оттуда не готов.
        /// </summary>
        [Test]
        public void StorageRefusal_BecomesAVisibleResult_NotAnException()
        {
            DurableLane<string> lane = Lane();
            _storage.WriteRefuses = true;

            QueueEnqueueResult result = lane.Enqueue("fact", "alpha", "1", target: "t");

            Assert.IsFalse(result.Accepted);
            Assert.AreEqual(QueueEnqueueStatus.RejectedStorageFailure, result.Status);
            Assert.AreEqual(0, lane.Count, "обещать доставку записи, которой нет, нельзя");
            Assert.IsNotEmpty(_log.Errors);
        }

        [Test]
        public void StorageThatThrows_IsCaughtToo_BecauseWeCannotRelyOnItsManners()
        {
            DurableLane<string> lane = Lane();
            _storage.WriteThrows = true;

            QueueEnqueueResult result = default;
            // Лямбда именно блочная: выражение-присваивание разрешается компилятором неоднозначно
            // между TestDelegate и Func<T>, и сборка падала на пустом месте.
            Assert.DoesNotThrow(() => { result = lane.Enqueue("fact", "alpha", "1", target: "t"); });

            Assert.AreEqual(QueueEnqueueStatus.RejectedStorageFailure, result.Status);
        }

        /// <summary>
        /// Битое содержимое удаляется и попадает в журнал. Молча вернуть «записей нет» — значит
        /// потерять данные так, что этого не видно нигде, а битое значение остаётся лежать и мешать
        /// каждому следующему запуску.
        /// </summary>
        [Test]
        public void UnreadableContent_IsRemovedAndReported()
        {
            _storage.Seed(QueueTestFactory.Key, "{ this is not our format");

            DurableLane<string> lane = Lane();

            Assert.AreEqual(0, lane.Count);
            Assert.IsNotEmpty(_log.Errors);
            StringAssert.DoesNotContain("not our format", _storage.Raw(QueueTestFactory.Key) ?? string.Empty);
        }

        /// <summary>
        /// Формат хранения принадлежит приложению: у переезжающей на библиотеку очереди он уже лежит
        /// на устройствах пользователей, и подменить кодек обязано быть дешевле, чем мигрировать
        /// чужие данные.
        /// </summary>
        [Test]
        public void SubstitutedCodec_IsActuallyUsedForBothWritingAndReading()
        {
            MarkerCodec codec = new();
            DurableLane<string> lane = Lane(codec: codec);

            lane.Enqueue("fact", "alpha", "hello", target: "t");

            string raw = _storage.Raw(QueueTestFactory.Key);
            StringAssert.StartsWith(MarkerCodec.Marker, raw, "писал именно подменённый кодек");
            Assert.Greater(codec.Encodes, 0);

            DurableLane<string> reloaded = Lane(codec: codec);

            Assert.AreEqual(1, reloaded.Count);
            Assert.AreEqual("hello", reloaded.Records[0].Payload);
            Assert.AreEqual("t", reloaded.Records[0].Target);
            Assert.Greater(codec.Decodes, 0, "читал тоже он");
        }

        [Test]
        public async Task Queue_SurvivesRestartWithItsCountersIntact()
        {
            RecordingDispatcher dispatcher = new(batch =>
                QueueResponse.FromServer(RecordingDispatcher.Endpoint(batch), 500));
            DurableLane<string> lane = QueueTestFactory.Lane(
                _storage, dispatcher, new ScriptedPolicy(_ => QueueRetryDecision.RetryLater()),
                _clock, _log);

            lane.Enqueue("fact", "alpha", "1", QueueRecordClass.Event, "op-7", "t");
            await lane.PumpAsync();

            DurableLane<string> reloaded = Lane();

            Assert.AreEqual(1, reloaded.Count);
            Assert.AreEqual("op-7", reloaded.Records[0].IdempotencyKey);
            Assert.AreEqual(1, reloaded.Records[0].Attempts);
            Assert.AreEqual("t", reloaded.Records[0].Target);
        }

        /// <summary>
        /// Кодек «свой формат»: строки вида <c>kind|stream|target|payload</c>. Ничего не умеет, кроме
        /// как доказать, что библиотека пользуется именно им, а не своим умолчанием.
        /// </summary>
        private sealed class MarkerCodec : IQueueCodec<string>
        {
            public const string Marker = "MARKER/1\n";

            public int Encodes { get; private set; }

            public int Decodes { get; private set; }

            public string Encode(IReadOnlyList<QueueRecord<string>> records)
            {
                Encodes++;
                StringBuilder builder = new(Marker);
                for (int i = 0; i < records.Count; i++)
                {
                    QueueRecord<string> record = records[i];
                    builder.Append(record.Kind).Append('|')
                        .Append(record.Stream).Append('|')
                        .Append(record.Target).Append('|')
                        .Append(record.Payload).Append('\n');
                }

                return builder.ToString();
            }

            public bool TryDecode(string raw, out List<QueueRecord<string>> records)
            {
                Decodes++;
                records = new List<QueueRecord<string>>();
                if (!raw.StartsWith(Marker, StringComparison.Ordinal))
                {
                    return false;
                }

                string[] lines = raw.Substring(Marker.Length)
                    .Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (string line in lines)
                {
                    string[] parts = line.Split('|');
                    records.Add(new QueueRecord<string> { Kind = parts[0], Stream = parts[1], Target = parts[2], Payload = parts[3] });
                }

                return true;
            }
        }
    }
}
