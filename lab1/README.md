# Отчёт по лабораторной работе 1. Выполнила Жданова Валерия 6401-010302D

## 1. Введение

**Apache Spark** — это фреймворк с открытым исходным кодом для распределённой обработки данных, ориентированный на работу в оперативной памяти. Он обеспечивает высокую производительность для задач ETL, машинного обучения и потоковой обработки.

В работе используются наборы данных:
- `trip.csv` — информация о поездках на велосипедах (длительность, станции, велосипед, пользователь).
- `station.csv` — данные о станциях (ID, название, координаты).

## 2. Настройка окружения

Для выполнения работы использовалась среда **Google Colab**, в которой предварительно установлен PySpark. 

### Установка PySpark
```python
!pip install pyspark
```

### Инициализация SparkSession
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("BikeAnalysis") \
    .getOrCreate()
```
## 3. Загрузка данных
```python
trips = spark.read.format('csv').option('header', 'true').load("trip.csv")
stations = spark.read.format('csv').option('header', 'true').load("station.csv")

# Очистка, оставляем только строки, где duration число
trips = trips.filter(F.col("duration").rlike("^[0-9]+$"))
```
## 4. Выполнение заданий
## Задание 1. Найти велосипед с максимальным временем пробега
Логика решения: cуммируем длительность всех поездок для каждого bike_id, сортируем по убыванию и берём первый.

```python
from pyspark.sql import functions as F
from pyspark.sql import types as t

bike_max_duration = (
    trips
    .groupBy("bike_id")
    .agg(F.sum(F.col("duration").cast(t.IntegerType())).alias("total_duration"))
    .orderBy(F.col("total_duration").desc())
)
bike_max_duration.show(1)
top_bike_id = bike_max_duration.first()["bike_id"]
```
Результат: 

![alt text](image-2.png)

Вывод: велосипед с ID 535 набрал суммарное время пробега 36 897 410 секунд (более 427 суток).

## Задание 2. Найти наибольшее геодезическое расстояние между станциями
Логика решения:
* Берём координаты станций (lat, long).
* Делаем декартово произведение stations × stations, отфильтровывая пары a.id < b.id.
* Для каждой пары вычисляем расстояние в километрах с помощью библиотеки geopy.
* Сортируем по убыванию и берём максимум.

```python
from geopy.distance import geodesic

# Подготовка координат
st_data = stations.select(
    F.col("id"),
    F.col("lat").cast(t.DoubleType()),
    F.col("long").cast(t.DoubleType())
)

# Декартово произведение с фильтром
st_pairs = st_data.alias("a").crossJoin(st_data.alias("b")).filter("a.id < b.id")

# UDF для расчёта расстояния
@F.udf(returnType=t.DoubleType())
def dist_udf(lat_a, lon_a, lat_b, lon_b):
    if all(v is not None for v in [lat_a, lon_a, lat_b, lon_b]):
        return geodesic((lat_a, lon_a), (lat_b, lon_b)).kilometers
    return 0.0

# Вычисление и максимум
max_geo_dist = (
    st_pairs
    .withColumn("distance_km", dist_udf("a.lat", "a.long", "b.lat", "b.long"))
    .orderBy(F.col("distance_km").desc())
)
max_geo_dist.select("a.id", "b.id", "distance_km").show(1)
```
Результат: ![alt text](image-3.png)
Вывод:
Максимальное расстояние между станциями — около 69.92 км (станции с ID 16 и 60).

## Задание 3. Найти путь велосипеда с максимальным временем пробега

Логика решения:
* Отфильтровать поездки для bike_id = 535.
* Выбрать start_station_name и end_station_name.
* Отсортировать по id (порядковый номер поездки).
* Вывести полный список.

```python
bike_path = (
    trips
    .filter(F.col("bike_id") == top_bike_id)
    .select("id", "start_station_name", "end_station_name")
    .orderBy(F.col("id").cast(t.IntegerType()))
)
bike_path.show(bike_path.count(), truncate=False)
```
Результат (фрагмент): 
![alt text](image-5.png)
Вывод: траектория велосипеда 535 охватывает множество станций Сан-Франциско, часто возвращаясь к ключевым узлам (Caltrain, Ferry Building, Market Street).

## Задание 4. Найти количество велосипедов в системе
Логика решения:  подсчитать количество уникальных bike_id в таблице поездок.

```python
total_bikes = trips.select("bike_id").distinct().count()
print(f"Количество уникальных велосипедов: {total_bikes}")
```
Результат: ![alt text](image-6.png)

## Задание 5. Найти пользователей, потративших на поездки более 3 часов
Логика решения:
* Сгруппировать по zip_code (используется как идентификатор пользователя).
* Суммировать длительность поездок.
* Отфильтровать сумму > 10800 секунд (3 часа).
* Отсортировать по убыванию.

```python
pro_users = (
    trips
    .groupBy("zip_code")
    .agg(F.sum(F.col("duration").cast(t.IntegerType())).alias("total_time"))
    .filter(F.col("total_time") > 10800)  # 3 часа = 10800 секунд
    .orderBy(F.col("total_time").desc())
)
pro_users.show()
```
Результат: ![alt text](image-7.png)
