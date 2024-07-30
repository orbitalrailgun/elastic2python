# функция вытаскивает из ответа эластика (прямой запрос без агрегаций) данные в лист диктов
# работает только с полями fields
def get_data(input_):
    output = []
    if "hits" in input_:
        if "hits" in input_["hits"]:
            for data in input_['hits']['hits']:
                if 'fields' in data:
                    if isinstance(data['fields'], dict):
                        new_node = {}
                        new_node["_id"] = data['_id']
                        for key in data['fields']:
                            #key, "->", likes[key]
                            if isinstance(data['fields'][key], list):
                                if len(data['fields'][key]) == 1:
                                    new_node[key] = data['fields'][key][0]
                                    continue
                            new_node[key] = data['fields'][key]
                            
                        output.append(new_node)
    return output


# функция обработки бакета, является подфункцией buckets_proc
def bucket_proc(bucket):

    column_name = None
    data = {}
    next_bucket = None
    next_bucket_list = []
    if isinstance(bucket, dict):
        for key in bucket.keys():
            if isinstance(bucket[key], dict): # мы нашли следующий слой
                #next_bucket = key
                if "buckets" in bucket[key]:
                    next_bucket = key
                    next_bucket_list = bucket[key]["buckets"]
                else:
                    # Это тоже данные
                    if "value_as_string" in bucket[key]:
                        data[key] = bucket[key]["value_as_string"]
                    elif "value" in bucket[key]:
                        data[key] = bucket[key]["value"]
            elif key == "key": # имя столбца
                    column_name = bucket[key]
            else:
                 data[key] = bucket[key]
    return column_name, data, next_bucket, next_bucket_list
                    
# функция обработки бакетов, обрабатывает данные в агрегационных запросах
def buckets_proc(buckets_list, layer, output_dict, output_list):
    if isinstance(buckets_list, list):
        import copy
        for bucket in buckets_list:
            if layer == 0:
                output_dict = {"columns":{}}
            #print(bucket)   
            bucket_data = bucket_proc(bucket)
            #print(bucket_data)
            if bucket_data[2] is not None:
                output_dict["columns"][layer] = bucket_data[0]
                buckets_proc(bucket_data[3], layer+1, copy.deepcopy(output_dict), output_list)
            else:
                output_dict["columns"][layer] = bucket_data[0]
                buf = copy.deepcopy(output_dict)
                buf["data"] = bucket_data[1]                
                output_list.append(buf)
                
# функция преобразует отправляемый параметр aggs в заголовки итоговой таблички
def request_aggs_to_headers_proc(input_node, output_list, output_dict):
    import copy
    last_node_flag = True
    next_node_key = ""
    next_input_node_key = ""
    debug = False
    for agg_key in input_node.keys():
        node = input_node[agg_key]
        last_node_flag = True
        
        if debug: print(agg_key)
        for key in node.keys():
            if key != "aggs":
                node_value = node[key]
                for key_value in node_value:
                    if key_value == "field":
                        if debug: print(node_value[key_value] + "_" +key)
                        output_list.append(node_value[key_value] + "_" +key)
                        output_dict[agg_key] = node_value[key_value] + "_" +key
            else:
                last_node_flag = False
                next_node_key = key
                next_input_node_key = agg_key
    if last_node_flag == False:
        request_aggs_to_headers_proc(input_node[next_input_node_key][next_node_key], output_list, output_dict)        

# вытаскивает данные из ответа по агрегационному запросу
def get_data_aggs(input_, request_aggs): # можно доработать имена столбцов
    raw_output = []
    raw_node = input_
    if "rawResponse" in input_:
        raw_node = input_["rawResponse"]
    
    if "aggregations" in raw_node:
            aggr = raw_node["aggregations"]
            if isinstance(aggr, dict):
                key = list(aggr.keys())[0]
                layer = 0
                if "buckets" in aggr[key]:
                    buckets_proc(aggr[key]["buckets"], 0, {"columns":{}}, raw_output)    
    output = []

    #header_of_columns
    header_list = []
    header_dict = {}
    request_aggs_to_headers_proc(request_aggs,header_list,header_dict)

    # print(raw_output)
    # print(header_list)
    # print(header_dict)

    # тут много приседаний по предобразованию данных из ответа эластика в табличку
    # данные приходят в ужасно нелогичном виде, просто посмотри raw_output и header_list
    # там есть 2 блока, обрабатываемых по-разному
    # бакеты поиска эластика и метрики, они преобразуются в столбцы по-разному
    # бакеты обрабатываются с перезаписыванием
    for raw in raw_output:
        node = {}
        header_list_position = 0

        columns_buf = -1
        last_key = ""

        for i, columns_key in enumerate(raw["columns"].keys()):
            node[header_list[columns_key]] = raw["columns"][columns_key]


        for i, data_key in enumerate(raw["data"].keys()):
            if data_key in header_dict:
                node[header_dict[data_key]] = raw["data"][data_key]
            else:
                node[data_key] = raw["data"][data_key]
            
        output.append(node)
    return output

def data_taxi(elastic_client, index, query, sort, fields, size, search_after, search_after_shift, debug = False):
    output_data = []
    debug_flag = debug
    if debug_flag:
        print("Получаем первичные данные")
    # сначала делаем первый запрос, получаем первый кусок данных
    try:
        response = elastic_client.search(
            index=index,
            body={
                        "query" : query,
                        "sort" : sort,
                        "fields" : fields,
                        "size" : size,
                        "search_after":search_after
                    }
        )
        output_data = get_data(dict(response))
    except BaseException as e:
        print("Ошибка выполнения запроса (первичное получение данных)", e)
        return []
    # проверяем первый полученный кусок данных, если данных столько, сколько указано в size
    # то скорее всего в запросе есть ещё, а значит надо сдвинуть поле search_after и повторить запрос
    if len(output_data) == size:
        sort_fields = [list(x.keys())[0] for x in sort] # по каким полям сортировка? парсим конструкцию sort
        taxi_step = 1
        while(True):
            if debug_flag:
                print("Получаем данные итерационно, шаг", taxi_step)
            search_after = [output_data[search_after_shift][sort_fields[0]]]
            try:
                response = elastic_client.search(
                    index=index, 
                    body={
                        "query" : query,
                        "sort" : sort,
                        "fields" : fields,
                        "size" : size,
                        "search_after":search_after
                    }
                )
                new_data = get_data(dict(response))
            except BaseException as e:
                print("Ошибка выполнения запроса (итеративное получение данных)", e)
                return []

            output_data = output_data + new_data
            if debug_flag:
                print("Получено данных", len(new_data))
                print("Текущий search_after", str(search_after))
            if len(new_data) != size:
                break
            taxi_step = taxi_step + 1    
    
    return output_data

def data_taxi_aggs(elastic_client, index, query, aggs, debug = False, size = 0):
    output_data = []
    debug_flag = debug
    if debug_flag:
        print("Делаем запрос агрегации")
    # сначала делаем первый запрос, получаем первый кусок данных
    try:
        response = elastic_client.search(
                index=index, 
                body = {
                    "query":query, 
                    "size":size,
                    "aggs":aggs
                }
            )
        if debug_flag:
            print("lib",response)
        output_data = get_data_aggs(dict(response), aggs)
    except BaseException as e:
        print("data_taxi_aggs error", e)
    return output_data

def data_taxi_csv_downloader(elastic_client, index, query, sort, fields, size, search_after, search_after_shift, filename, writemode):
    import pandas # интерфейс для csv и как способ удалить дубликаты
    output_data = []
    debug_flag = True
    if debug_flag:
        print("Получаем первичные данные")
    # сначала делаем первый запрос, получаем первый кусок данных
    try:
        response = elastic_client.search(
            index=index,
            body={
                        "query" : query,
                        "sort" : sort,
                        "fields" : fields,
                        "size" : size,
                        "search_after":search_after
                    }

        )
        output_data = get_data(dict(response))
    except BaseException as e:
        print("Ошибка выполнения запроса (первичное получение данных)", e)
        return []
    # "append" -- значит каждую итерацию сразу записываем в файл и тем самым экономим ОЗУ. Потребуется в дальнейшем удалить дубликаты по полю _id.
    # "full" -- значит каждую итерацию пишем в ОЗУ. Дубликаты удалим через pandas сразу.
    if writemode == "append":
        pandas.DataFrame(output_data).to_csv(filename)
    # проверяем первый полученный кусок данных, если данных столько, сколько указано в size
    # то скорее всего в запросе есть ещё, а значит надо сдвинуть поле search_after и повторить запрос
    if len(output_data) == size:
        sort_fields = [list(x.keys())[0] for x in sort] # по каким полям сортировка? парсим конструкцию sort
        taxi_step = 1
        while(True):
            if debug_flag:
                print("Получаем данные итерационно, шаг", taxi_step)
                
            # блок обновления search_after 
            if writemode == "append" and taxi_step > 1:
                search_after = [new_data[search_after_shift][sort_fields[0]]]
            else:
                search_after = [output_data[search_after_shift][sort_fields[0]]]

            # блок запроса
            try:
                response = elastic_client.search(
                    index=index,
                    body={
                                "query" : query,
                                "sort" : sort,
                                "fields" : fields,
                                "size" : size,
                                "search_after":search_after
                            }
                )
                new_data = get_data(dict(response))
            except BaseException as e:
                print("Ошибка выполнения запроса (итеративное получение данных)", e)
                return []

            if writemode == "append":
                pandas.DataFrame(new_data).to_csv(filename, mode='a', header=False)
                #output_data = output_data + new_data
            else:
                output_data = output_data + new_data
            if debug_flag:
                print("Получено данных", len(new_data))
                print("Текущий search_after", str(search_after))
            if len(new_data) != size:
                break
            taxi_step = taxi_step + 1    
    if writemode == "full":
        pandas.DataFrame(output_data).drop_duplicates(["_id"]).to_csv(filename)
    if debug_flag:
        print("Получение данных завершено")

