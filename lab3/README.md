# Отчет по лабораторной работе 3. Выполнила Жданова Валерия 6401-010302D

### Задание
Выполнить следующие задания из набора заданий репозитория https://github.com/ververica/flink-training-exercises:

* RideCleanisingExercise
* RidesAndFaresExercise
* HourlyTipsExerxise
* ExpiringStateExercise
### Порядок выполнения

#### 1. Подготовка окружения
- Клонирован репозиторий с учебными упражнениями
- Настроены пути к датасетам в ExerciseBase.java

#### 2. Реализация заданий

Задание 1: RideCleansingExercise
- Реализована фильтрация потока поездок
- Оставлены только поездки, начинающиеся и заканчивающиеся в пределах Нью-Йорка
- Использован встроенный утилитный класс GeoUtils для проверки координат

Задание 2: RidesAndFaresExercise
- Реализовано соединение двух потоков (поездки и оплаты) по идентификатору
- Использовано управляемое состояние ValueState для хранения ожидающих событий
- Обеспечена работа при любом порядке прихода данных

Задание 3: ExpiringStateExercise
- Настроены таймеры на 2 минуты для автоматической очистки состояния
- Непарные события отправлены в боковые выходы (side outputs)

Задание 4: HourlyTipsExercise
- Реализовано вычисление суммы чаевых для каждого водителя за час
- Найден водитель с максимальной суммой чаевых в каждом часе

### Результаты

Все четыре задания выполнены и прошли соответствующие тесты:
<img width="580" height="301" alt="image" src="https://github.com/user-attachments/assets/1e2262cc-122b-4b5c-ba0d-40bb5615d252" />
<img width="595" height="220" alt="image" src="https://github.com/user-attachments/assets/7df461b9-9b1e-46ef-a2ef-6c4f0cd99cfa" />
<img width="577" height="241" alt="image" src="https://github.com/user-attachments/assets/ceecc94f-30bb-40f8-8354-2122e0007758" />
<img width="581" height="231" alt="image" src="https://github.com/user-attachments/assets/150599ba-68f9-45ec-964b-46a4c0c73b51" />
