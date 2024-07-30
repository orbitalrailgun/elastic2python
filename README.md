# elastic2python

elastic2python это набор функций, которые позволяют удобно извлечь данные из elastic или opensearch с помощью python и представить их в виде list of dicts или сохранить в csv.

elastic2python is a set of functions that allow you to conveniently extract data from elastic or opensearch using python and present it as a list of dicts or save it in csv.
***
Для чего это нужно:
* позволяет работать с данными в pandas
* позволяет выгружать данные в csv

What is it for?:
* allows you to work with data in pandas
* allows you to upload data to csv
***
Пример использования представлен в файле elastic2python.ipynb

An example of usage is provided in the elastic 2 python.ipynb file
***
Для работы необходимы модули elasticsearch и opensearch-py соответственно.

The elasticsearch and opensearch-py modules are required for operation, respectively.
***
Основные функции, предлагаемые для использования:

The main functions offered for use:

### data_taxi -- получаем данные как в Discovery/we get the data as in Discovery

    elastic_client, # клиент подключения elasticsearch или opensearch-py / elasticsearch or opensearch-py connection client
    index, # индекс или датавью системы / the index or dataview of the system
    query, # DSL запрос, можно отладить и позаимствовать из блока inspect->request в интерфейсе kibana или opensearch / The DSL request can be debugged and borrowed from the inspect->request block in the kibana or opensearch interface
    sort, # параметры сортировки (search_after работает с первым по списку, лучше его оставить единственным), можно отладить и позаимствовать из блока inspect->request в интерфейсе kibana или opensearch /sorting parameters (search_after works with the first one in the list, it is better to leave it the only one), you can debug and borrow from the inspect->request block in the kibana or opensearch interface
    fields,  # список выделяемых полей, данные берутся только оттуда, можно отладить и позаимствовать из блока inspect->request в интерфейсе kibana или opensearch / the list of selected fields, data is taken only from there, can be debugged and borrowed from the inspect->request block in the kibana or opensearch interface
    size, # сколько записей будет запрошено за раз. Обычно это 10000, но для управления нагрузкой на сервер можно выставлять меньше. Главное, чтобы по модулю оно было больше search_after_shift. / how many records will be requested at a time. This is usually 10,000, but you can set less to manage the load on the server. The main thing is that it should be larger in modulus than search_after_shift.
    search_after, # первый ограничитель забираемых данных, обычно равен "lte" блоку при сортировке по времени, крайняя минимальная граница диапазона, далее она будет сдвигаться от итерации до итерации, пока не достигнет максимальной. / the first limiter of the data being collected is usually equal to the "lte" block when sorted by time, the extreme minimum limit of the range, then it will shift from iteration to iteration until it reaches the maximum
    search_after_shift, # обратный сдвиг при формировании search_after для следующей итерации. Нужен при высокой плотности событий, когда близко располагаемые документы оказываются с идентичным параметром, указанным в sort. Обеспечивает корректность пагинации. Обычно -10 хватает, главное, чтобы по модулю был меньше size. / reverse shift when forming search_after for the next iteration. It is needed for a high density of events, when closely located documents turn out to have an identical parameter specified in sort. Ensures the correctness of the pagination. Usually -10 is enough, the main thing is that the modulus should be less than size.
    debug = False # параметр для отладки, включает текстовые сообщения. / the parameter for debugging enables text messages.

На выходе будет / The output will be [{...},{...},...].

### data_taxi_aggs -- получаем данные как в Aggregation Based->Data Table/we get the data as in Aggregation Based->Data Table

    elastic_client, # клиент подключения elasticsearch или opensearch-py / elasticsearch or opensearch-py connection client
    index, # индекс или датавью системы / the index or dataview of the system
    query, # DSL запрос, можно отладить и позаимствовать из блока inspect->request в интерфейсе kibana или opensearch / The DSL request can be debugged and borrowed from the inspect->request block in the kibana or opensearch interface
    aggs, # aggs блок, который можно отладить и позаимствовать из блока inspect->request в интерфейсе kibana или opensearch. Это можно найти в Visualize Library->Create Visualization->Aggregation based->Data table->index/dataview. / An aggs block that can be debugged and borrowed from the inspect->request block in the kibana or opensearch interface. This can be found in Visualize Library->Create Visualization->Aggregation based->Data table->index/dataview.
    debug = False, # параметр для отладки, включает текстовые сообщения . / the parameter for debugging enables text messages.
    size = 0 # Параллельно забираемые данные, функция с ними не работает, поэтому 0. / The data is being collected in parallel, the function does not work with them, therefore 0.

На выходе будет / The output will be [{...},{...},...].

### data_taxi_csv_downloader -- получаем данные как в Discovery и сохраняем в csv/we get the data as in Discovery and save it in csv

    elastic_client, # аналогично data_taxi / similar to data_taxi
    index, # аналогично data_taxi / similar to data_taxi 
    query, # аналогично data_taxi / similar to data_taxi 
    sort, # аналогично data_taxi / similar to data_taxi  
    fields, # аналогично data_taxi / similar to data_taxi  
    size, # аналогично data_taxi / similar to data_taxi  
    search_after, # аналогично data_taxi / similar to data_taxi  
    search_after_shift, # аналогично data_taxi / similar to data_taxi  
    filename, # в какой файл сохраняем получаемые данные / in which file do we save the received data
    writemode # выбор способа записи. "append" -- значит каждую итерацию сразу записываем в файл и тем самым экономим ОЗУ. Потребуется в дальнейшем удалить дубликаты по полю _id. "full" -- значит каждую итерацию пишем в ОЗУ. Дубликаты удалим через pandas сразу. / choosing the recording method. "append" means we immediately write each iteration to a file and thereby save RAM. It will be necessary to remove duplicates in the _id. "full" field in the future -- so we write each iteration to RAM. Duplicates will be deleted via pandas immediately.


На выходе будет csv файл. / The output will be csv file.
