using System;

namespace Neoxider.DurableQueue
{
    /// <summary>
    /// Адрес ручки: метод и ШАБЛОН пути, без подставленных значений.
    ///
    /// <para>Шаблон, а не готовый путь, по двум причинам. Первая: решение политики повторов часто
    /// зависит от того, какая это ручка, — один и тот же код ответа на разных ручках значит разное,
    /// и правило «на этой ручке повтор безопасен» пишется по шаблону, а не по пути с подставленным
    /// идентификатором. Вторая: путь с подставленными значениями — это данные владельца записи, и
    /// в журнал их тащить незачем.</para>
    /// </summary>
    public readonly struct QueueEndpoint : IEquatable<QueueEndpoint>
    {
        public QueueEndpoint(string method, string pathTemplate)
        {
            Method = method ?? string.Empty;
            PathTemplate = pathTemplate ?? string.Empty;
        }

        public string Method { get; }

        public string PathTemplate { get; }

        public bool Equals(QueueEndpoint other)
        {
            return string.Equals(Method, other.Method, StringComparison.Ordinal)
                   && string.Equals(PathTemplate, other.PathTemplate, StringComparison.Ordinal);
        }

        public override bool Equals(object obj) => obj is QueueEndpoint other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Method?.GetHashCode() ?? 0) * 397) ^ (PathTemplate?.GetHashCode() ?? 0);
            }
        }

        public override string ToString() => $"{Method} {PathTemplate}";
    }
}
