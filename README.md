## Запуск на Windows

- Установка [Python 3.10](https://www.python.org/downloads/release/python-3100/). Не забудьте поставить галочку "Add Python to Path".
- Установить [git](https://git-scm.com/download/win) или напрямую скачать zip файл.

```bash
git clone https://github.com/cardisnotvalid/hltv-stats-scraper.git
cd stats_scraper
```

- Установка требуемых библиотек.

```bash
python.exe -m pip install --upgrade pip
pip install -r requirements.txt
```

- Установка webdriver для playwright

```bash
playwright install
```

- Запуск скрипта через `start.bat`.

## Настройка

Настройки скрипта находятся в `config.yaml`.

```yaml
# -- Developer Settings --
# Вывод в консоль дополнительной информации
debug: True
```

## Результат

Скрипт сохраняет все данные в папку `./output`.