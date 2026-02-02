import os
import time
import tracemalloc
from collections import Counter

def extract_ip(line):
    # Извлечение IP-адреса из строки лога"""
    # IP находится до первого пробела
    return line.split()[0] if line.strip() else None


def naive_solution(filename):
    # Наивное решение - чтение всего файла в память
    start_time = time.time()
    ip_counter = Counter()

    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            ip = extract_ip(line)
            if ip:
                ip_counter[ip] += 1

    # Получаем топ-10
    top_10 = ip_counter.most_common(10)

    # Время работы
    execution_time = time.time() - start_time

    # Вывод топ-10 ip и время работы программы
    return top_10, execution_time



def mapper(chunk):
    # Функция Map: обрабатывает чанк данных
    # Возвращает список пар (ip, 1)

    local_count = {}
    for line in chunk:
        ip = extract_ip(line)
        if ip:
            local_count[ip] = local_count.get(ip, 0) + 1
    return local_count.items()


def reducer(mapped_results):
    # Функция Reduce: суммирует результаты от всех Mapper'ов
    final_count = {}
    for ip, count in mapped_results:
        final_count[ip] = final_count.get(ip, 0) + count
    return final_count


def mapreduce_solution(filename, chunk_size=10000):
    # Решение с использованием принципа MapReduce
    start_time = time.time()

    # Этап 1: Чтение и разбиение на чанки
    def read_chunks():
        # Генератор, читающий файл по чанкам
        with open(filename, 'r', encoding='utf-8') as f:
            chunk = []
            for line in f:
                chunk.append(line)
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk

    # Этап 2: Map (последовательно, но можно распараллелить)
    mapped_results = []
    for chunk in read_chunks():
        mapped_results.extend(mapper(chunk))

    # Этап 3: Shuffle (группировка по IP)
    shuffled = {}
    for ip, count in mapped_results:
        if ip not in shuffled:
            shuffled[ip] = []
        shuffled[ip].append(count)

    # Этап 4: Reduce
    reduced = {}
    for ip, counts in shuffled.items():
        reduced[ip] = sum(counts)

    # Получаем топ-10
    top_10 = sorted(reduced.items(), key=lambda x: x[1], reverse=True)[:10]

    execution_time = time.time() - start_time

    return top_10, execution_time


def compare_solutions():
    # Сравнение двух решений
    files_to_test = [
        ('access_small.log', 'Маленький файл (1K строк)'),
        ('access.log', 'Большой файл (1.5M строк)')
    ]

    for filename, description in files_to_test:
        print(f"\n{'=' * 60}")
        print(f"Тестирование: {description}")
        print(f"{'=' * 60}")

        if os.path.exists(filename):
            # Наивное решение
            top_naive, time_naive = naive_solution(filename)
            print(f"\nНаивное решение (время: {time_naive:.2f} сек):")
            for i, (ip, count) in enumerate(top_naive, 1):
                print(f"  {i:2}. {ip:15} - {count:6} запросов")

            # MapReduce решение
            top_mr, time_mr = mapreduce_solution(filename)
            print(f"\nMapReduce решение (время: {time_mr:.2f} сек):")
            for i, (ip, count) in enumerate(top_mr, 1):
                print(f"  {i:2}. {ip:15} - {count:6} запросов")

            # Сравнение производительности
            print(f"\nСравнение производительности:")
            print(f"  MapReduce медленнее в {time_mr / time_naive:.2f} раз")

            # Проверка корректности
            print(f"\nКорректность результатов:")
            naive_dict = dict(top_naive)
            mr_dict = dict(top_mr)

            # Сравниваем топ-3 для проверки
            for ip, count in top_naive[:3]:
                if ip in mr_dict and abs(mr_dict[ip] - count) <= count * 0.1:
                    print(f"  ✓ IP {ip}: результаты совпадают")
                else:
                    print(f"  ✗ IP {ip}: расхождение результатов")
        else:
            print(f"Файл {filename} не найден!")


print(compare_solutions())

def measure_memory_usage(solution_func, filename):
    """Измерение потребления памяти"""
    tracemalloc.start()

    result, execution_time = solution_func(filename)

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return result, execution_time, peak / 1024 / 1024  # MB

print(measure_memory_usage(naive_solution, 'access_small.log'))
print(measure_memory_usage(naive_solution, 'access.log'))
print(measure_memory_usage(mapreduce_solution, 'access_small.log'))
print(measure_memory_usage(mapreduce_solution, 'access.log'))
