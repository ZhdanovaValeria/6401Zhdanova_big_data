# Отчет по лабораторной работе 2. Выполнила Жданова Валерия 6401-010302D

## 1. Цель работы

Сформировать отчет с информацией о 10 наиболее популярных языках программирования по итогам года за период с 2010 по 2020 годы. Отчет должен отражать динамику изменения популярности языков программирования и представлять собой набор таблиц "топ-10" для каждого года. Результат сохранить в формате Apache Parquet.

### 2. Подготовка окружения

```python
# Установка и импорт необходимых библиотек
!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
import re
```

### Создание Spark-сессии

```python
spark = SparkSession.builder \
    .appName("Lab2_ProgrammingLanguagesReport") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

### Загрузка данных постов

```python
!wget https://git.ai.ssau.ru/tk/big_data/raw/branch/master/data/posts_sample.xml

postsData = spark.read.format('xml') \
    .option('rowTag', 'row') \
    .option("timestampFormat", 'y/M/d H:m:s') \
    .load('posts_sample.xml')
```

Результаты анализа данных:
- Всего постов: 46 006
- Период данных: с 2008 по 2019 год
- Постов за период 2010-2020: 44 419

### Загрузка справочника языков программирования

```python
!wget https://git.ai.ssau.ru/tk/big_data/raw/branch/master/data/programming-languages.csv

languagesData = spark.read.format('csv') \
    .option('header', 'true') \
    .option("inferSchema", True) \
    .load('programming-languages.csv') \
    .dropna()
```

Результат: загружено 699 уникальных языков программирования

### 3. Выполнение работы
### Фильтрация данных за нужный период

```python
dates = ("2010-01-01", "2020-12-31")
posts_by_date = postsData.filter(F.col("_CreationDate").between(*dates))
```

### Обработка данных с использованием RDD API


```python
def includes_name(x):
    tag = None
    for name in language_names:
        n = '<' + name.lower() + '>'
        if n in str(x._Tags).lower():
            tag = name
            break
    if tag is None:
        tag = 'No'
    
    year = int(x._CreationDate[:4])
    return (year, tag)
```
Преобразование данных:
1. Преобразование DataFrame в RDD
2. Применение функции includes_name к каждой записи
3. Фильтрация записей без определенного языка

```python
posts_by_date_rdd = posts_by_date.rdd \
    .map(includes_name) \
    .filter(lambda x: x[1] != 'No')
```

### Группировка и подсчет популярности

```python
posts_by_date_rdd_group = posts_by_date_rdd \
    .keyBy(lambda row: (row[0], row[1])) \
    .aggregateByKey(0, lambda x, y: x + 1, lambda x1, x2: x1 + x2)
```

### Формирование топ-10 для каждого года

Для каждого доступного года:
1. Фильтрация данных по году
2. Сортировка по убыванию количества упоминаний
3. Выбор первых 10 записей
4. Создание DataFrame с полями: Year, Language, Count
5. Сохранение в отдельный Parquet-файл

```python
for year in available_years:
    year_data = posts_by_date_rdd_group.filter(lambda x: x[0][0] == year)
    top10 = year_data.sortBy(lambda x: x[1], ascending=False).take(10)
    
    year_df = spark.createDataFrame(
        [Row('Year', 'Language', 'Count')(year, lang, count) 
         for (_, lang), count in top10]
    )
    
    output_path = f"top10_languages/{year}"
    year_df.write.mode("overwrite").parquet(output_path)
```


### 4. Результаты
<img width="786" height="727" alt="image" src="https://github.com/user-attachments/assets/50392cff-36b0-4847-a78a-5c76c1a30a9d" />


<img width="494" height="641" alt="image" src="https://github.com/user-attachments/assets/22cc7837-0d87-4bcc-9d72-6ba80b77806c" />

<img width="298" height="428" alt="image" src="https://github.com/user-attachments/assets/5ff60993-61a0-43b4-9a54-7efd2730f4de" />

