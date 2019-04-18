# Задача

Этот `бонус-таск` базируется на задании https://github.com/spajic/task-4

В исходном задании вы импортировали файлы `small.json`, `medium.json` и `large.json` (`1k`, `10k` и `100k` `trip`-ов).

В этом задании нужно справиться с файлами `1M.json` (`codename mega`) и `10M.json` (`codename hardcore`)

- [mega](https://www.dropbox.com/s/mhc2pzgtt4bp485/1M.json.gz?dl=1)
- [hardcore](https://www.dropbox.com/s/h08yke5phz0qzbx/10M.json.gz?dl=1)

## Подсказки

### Мета-информация о данных

При реализации импорта нужно учесть наши инсайдерские знания о данных:
- первичным ключом для автобуса считаем `(model, number)`
- уникальных автобусов в файле `10M.json` ~ `10_000`
- ункикльных городов в файле `10M.json` ~ `100`
- сервисов ровно `10`, те что перечислены в `Service::SERVICES`

### Стриминг

Файл `10M.json` весит ~ `3Gb`.
Поэтому лучше не пытаться грузить его целиком в память и парсить.

Вместо этого лучше читать и парсить его потоково.

Это более-менее привычная схема, но знали ли вы, что в `Posgtres` тоже можно писать данные потоком?

Вот набросок потокового чтения из файла с потоковой записью в `Postgres`:

```ruby
@cities = {}

ActiveRecord::Base.transaction do
  trips_command =
    "copy trips (from_id, to_id, start_time, duration_minutes, price_cents, bus_id) from stdin with csv delimiter ';'"

  ActiveRecord::Base.connection.raw_connection.copy_data trips_command do
    File.open(file_name) do |ff|
      nesting = 0
      str = +""

      while !ff.eof?
        ch = ff.read(1) # читаем по одному символу
        case
        when ch == '{' # открывается объект, повышается вложенность
          nesting += 1
          str << ch
        when ch == '}' # закрывается объкет, понижается вложенность
          nesting -= 1
          str << ch
          if nesting == 0 # если закрылся объкет уровня trip, парсим и импортируем его
            trip = Oj.load(str)
            import(trip)
            progress_bar.increment
            str = +""
          end
        when nesting >= 1
          str << ch
        end
      end
    end
  end
end

def import(trip)
  from_id = @cities[trip['from']]
  if !from_id
    from_id = cities.size + 1
    @cities[trip['from']] = from_id
  end

  # ...

  # стримим подготовленный чанк данных в postgres
  connection.put_copy_data("#{from_id};#{to_id};#{trip['start_time']};#{trip['duration_minutes']};#{trip['price_cents']};#{bus_id}\n")
end
```

### Plan

- чистим базу
- идём по большому файлу
- по пути формируем в памяти вспомогательные справочники ограниченного размера (`cities`, `buses`, `buses_services`)
- сразу же стримим основные данные в базу (`trips`), чтобы не накапливать их
- после завершения файла сохраняем в базу сформированные справочники

### Notes

- можно использовать любые библиотеки для потоковой обработки `json` и вообще

## Как сдать задание

Довести до ума потоковый импорт данных.

Начать с отработки на маленьких файлах, оптимизировать до состояния, в котором файл `10M.json` импортируется за приемлемое время.

Сделать `PR` в этот репозиторий, где есть:
- [x] файлы с кодом, выполняющим импорт
- [x] описание процесса оптимизации и результатов
