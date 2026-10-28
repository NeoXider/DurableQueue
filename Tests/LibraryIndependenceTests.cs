using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using UnityEngine;

namespace Neoxider.DurableQueue.Tests
{
    /// <summary>
    /// Стражи независимости библиотеки.
    ///
    /// <para><b>Почему они здесь, а не в общих правилах проекта.</b> Правила проекта-хозяина
    /// сканируют только его собственный игровой код и папку сторонних пакетов не видят — то есть на
    /// эту библиотеку не срабатывает ни одно из них. Расширять их корни нельзя: у пакета свои нормы
    /// (собственный порт журнала вместо фасада хозяина — это норма, а не нарушение), и общее правило
    /// начало бы краснеть на нём законно. Поэтому стражи свои.</para>
    ///
    /// <para><b>Что именно они охраняют.</b> Риск «протечка частного в общее» вероятнее риска
    /// «абстракция ради абстракции»: ключ, скоупленный по чужой сущности, чужие коды ошибок, чужой
    /// журнал «на минутку». Смягчение обязано быть машинным, а не дисциплинарным — договорённость
    /// живёт до первого дедлайна.</para>
    /// </summary>
    public sealed class LibraryIndependenceTests
    {
        /// <summary>
        /// Типы хозяина и платформенные API, которых в библиотеке быть не может. Ищем по
        /// ОБРАЩЕНИЯМ в коде, а не по любому упоминанию: комментарии вырезаются, иначе страж
        /// покраснеет от объяснения, почему этих типов здесь нет.
        /// </summary>
        private static readonly string[] ForbiddenIdentifiers = { @"\bRedoLog\b", @"\bDebug\s*\.", @"\bUnityWebRequest\b", @"\bPlayerPrefs\b", @"api/v1" };

        /// <summary>
        /// Словарь предметной области хозяина. Ищется по всему тексту, включая комментарии: язык
        /// проекта протекает в общую библиотеку сперва через объяснения и лишь потом через типы.
        /// </summary>
        private static readonly string[] ForbiddenVocabulary = { @"\bstudents?\b", @"\blessons?\b", @"\bmissions?\b", @"\bbriefings?\b" };

        /// <summary>
        /// Корень пакета — по asmdef рантайма, а не по пути: пакет лежит и в <c>Assets</c>, и в <c>Packages</c>
        /// (установленный из git — в <c>Library/PackageCache</c>).
        /// </summary>
        private static string PackageRoot
        {
            get
            {
                string asmdef = UnityEditor.Compilation.CompilationPipeline
                    .GetAssemblyDefinitionFilePathFromAssemblyName("Neoxider.DurableQueue");
                if (string.IsNullOrEmpty(asmdef))
                {
                    return string.Empty;
                }

                UnityEditor.PackageManager.PackageInfo package =
                    UnityEditor.PackageManager.PackageInfo.FindForAssetPath(asmdef);
                return package != null
                    ? package.resolvedPath
                    : Path.GetDirectoryName(Path.GetDirectoryName(Path.GetFullPath(asmdef)));
            }
        }

        [Test]
        public void PackageIsWhereTheTestsExpectIt()
        {
            Assert.IsTrue(
                Directory.Exists(PackageRoot),
                $"Библиотека не найдена по пути '{PackageRoot}'. Если пакет переехал — поправить " +
                "путь здесь, иначе стражи независимости молча перестанут что-либо проверять.");
        }

        /// <summary>
        /// Ссылка на сборку хозяина — единственная защита, которая работает на уровне компиляции.
        /// Правила направления зависимостей у проекта её не ловят: ребро «библиотека — платформа
        /// хозяина» цикла не создаёт и под их префиксы не подпадает, поэтому проходит молча.
        /// </summary>
        [Test]
        public void RuntimeAssembly_ReferencesNothingFromTheHostProject()
        {
            string asmdefPath = Path.Combine(PackageRoot, "Runtime", "Neoxider.DurableQueue.asmdef");
            Assert.IsTrue(File.Exists(asmdefPath), $"Нет '{asmdefPath}'");

            JObject asmdef = JObject.Parse(File.ReadAllText(asmdefPath));
            JToken references = asmdef["references"];
            Assert.IsNotNull(references);

            foreach (JToken reference in references)
            {
                string name = reference.Value<string>();
                Assert.IsFalse(
                    name.StartsWith("RedoSchool", System.StringComparison.OrdinalIgnoreCase),
                    $"Библиотека ссылается на сборку проекта-хозяина '{name}'. Это конец её " +
                    "независимости: дальше внутрь приезжают и типы, и понятия хозяина.");
            }
        }

        [Test]
        public void TestAssembly_IsVisibleToTheRunner()
        {
            string asmdefPath = Path.Combine(PackageRoot, "Tests", "Neoxider.DurableQueue.Tests.asmdef");
            Assert.IsTrue(File.Exists(asmdefPath), $"Нет '{asmdefPath}'");

            string references = JObject.Parse(File.ReadAllText(asmdefPath))["references"]?.ToString();
            Assert.IsNotNull(references);

            // Без этих ссылок раннер сборку не находит: прогон выглядит зелёным, не выполнив ни
            // одного теста, — то есть страж независимости существует, но ничего не сторожит.
            StringAssert.Contains("UnityEngine.TestRunner", references);
            StringAssert.Contains("UnityEditor.TestRunner", references);
        }

        [Test]
        public void RuntimeSources_DoNotTouchHostTypesOrPlatformStorage()
        {
            List<string> violations = new();
            foreach (string file in RuntimeSources())
            {
                string code = StripComments(File.ReadAllText(file));
                foreach (string pattern in ForbiddenIdentifiers)
                {
                    if (Regex.IsMatch(code, pattern))
                    {
                        violations.Add($"{Path.GetFileName(file)}: {pattern}");
                    }
                }
            }

            CollectionAssert.IsEmpty(
                violations,
                "В библиотеке появились типы хозяина или платформенное хранилище. У неё есть свои " +
                "порты ровно для этого; обращение к чужому типу делает её частью одного проекта.");
        }

        [Test]
        public void RuntimeSources_DoNotSpeakTheHostDomainLanguage()
        {
            List<string> violations = new();
            foreach (string file in RuntimeSources())
            {
                string text = File.ReadAllText(file);
                foreach (string pattern in ForbiddenVocabulary)
                {
                    if (Regex.IsMatch(text, pattern, RegexOptions.IgnoreCase))
                    {
                        violations.Add($"{Path.GetFileName(file)}: {pattern}");
                    }
                }
            }

            CollectionAssert.IsEmpty(
                violations,
                "В библиотеке появилось слово из предметной области проекта-хозяина. Соблазн " +
                "затащить сюда его сущности самый сильный — и именно ему нельзя поддаваться: " +
                "внутрь приходит просто строковый ключ, а чей он — решает адаптер.");
        }

        /// <summary>
        /// Зависимости объявлены явно. У соседних пакетов этого нет, и сходит им с рук только
        /// потому, что нужные пакеты в ЭТОМ репозитории уже стоят: у одного из них зависимость
        /// вообще приезжает транзитивно, то есть в чужом проекте её может не оказаться вовсе.
        /// Копировать эту привычку нельзя — библиотека объявляет обе свои зависимости сама.
        /// </summary>
        [Test]
        public void PackageManifest_DeclaresBothDependencies()
        {
            string manifestPath = Path.Combine(PackageRoot, "package.json");
            Assert.IsTrue(File.Exists(manifestPath), $"Нет '{manifestPath}'");

            JObject manifest = JObject.Parse(File.ReadAllText(manifestPath));
            JObject dependencies = manifest["dependencies"] as JObject;
            Assert.IsNotNull(dependencies, "В package.json нет блока dependencies");

            Assert.IsNotNull(dependencies["com.cysharp.unitask"], "Не объявлен com.cysharp.unitask");
            Assert.IsNotNull(
                dependencies["com.unity.nuget.newtonsoft-json"],
                "Не объявлен com.unity.nuget.newtonsoft-json");

            foreach (string required in new[] { "unity", "displayName", "description", "author", "license" })
            {
                Assert.IsNotNull(manifest[required], $"В package.json нет поля '{required}'");
            }
        }

        [Test]
        public void PackageCarriesItsPaperwork()
        {
            foreach (string file in new[] { "README.md", "CHANGELOG.md", "LICENSE.md" })
            {
                Assert.IsTrue(
                    File.Exists(Path.Combine(PackageRoot, file)),
                    $"Нет '{file}'. Пригодность к отдаче наружу — условие приёмки, а не «когда-нибудь причешем».");
            }
        }

        private static IEnumerable<string> RuntimeSources()
        {
            return Directory.GetFiles(
                Path.Combine(PackageRoot, "Runtime"), "*.cs", SearchOption.AllDirectories);
        }

        private static string StripComments(string code)
        {
            string withoutBlocks = Regex.Replace(code, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
            return Regex.Replace(withoutBlocks, @"//.*?$", string.Empty, RegexOptions.Multiline);
        }
    }
}
