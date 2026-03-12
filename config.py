# config.py

CONFIG = {
    "yandex_geocoder_key": "e27943f8-c65f-4361-8c7c-780e1bef1f63",
    "openweather_api_key": "e2104ba6dcfcdac3eb9fb28f20f85ec2",

    "cities": {
        "Астрахань": {
            "full_name": 'Астрахань',
            "osm_name": 'Астрахань',
            "center": [46.3498, 48.0408],
            "zoom": 12,
            "admin_level": '9',
        },
        "Санкт-Петербург": {
            "full_name": 'Санкт-Петербург',
            "osm_name": 'Санкт-Петербург',
            "center": [59.9343, 30.3351],
            "zoom": 11,
            "admin_level": '5',
            "preset_zones": [
                {'name': 'Адмиралтейский', 'type': 'district', 'osm_relation_id': 1114193},
                {'name': 'Василеостровский', 'type': 'district', 'osm_relation_id': 1114252},
                {'name': 'Выборгский', 'type': 'district', 'osm_relation_id': 1114354},
                {'name': 'Калининский', 'type': 'district', 'osm_relation_id': 1114806},
                {'name': 'Кировский', 'type': 'district', 'osm_relation_id': 1114809},
                {'name': 'Колпинский', 'type': 'district', 'osm_relation_id': 337424},
                {'name': 'Красногвардейский', 'type': 'district', 'osm_relation_id': 1114895},
                {'name': 'Красносельский', 'type': 'district', 'osm_relation_id': 363103},
                {'name': 'Кронштадтский', 'type': 'district', 'osm_relation_id': 1115082},
                {'name': 'Курортный', 'type': 'district', 'osm_relation_id': 1115366},
                {'name': 'Московский', 'type': 'district', 'osm_relation_id': 338636},
                {'name': 'Невский', 'type': 'district', 'osm_relation_id': 368287},
                {'name': 'Петроградский', 'type': 'district', 'osm_relation_id': 1114905},
                {'name': 'Петродворцовый', 'type': 'district', 'osm_relation_id': 367375},
                {'name': 'Приморский', 'type': 'district', 'osm_relation_id': 1115367},
                {'name': 'Пушкинский', 'type': 'district', 'osm_relation_id': 338635},
                {'name': 'Фрунзенский', 'type': 'district', 'osm_relation_id': 369514},
                {'name': 'Центральный', 'type': 'district', 'osm_relation_id': 1114902},
            ],
        },
        "Новосибирск": {
            "full_name": 'Новосибирск',
            "osm_name": 'Новосибирск',
            "center": [55.0084, 82.9357],
            "zoom": 12,
            "admin_level": '9',
            "preset_zones": [
                {'name': 'Дзержинский', 'type': 'district', 'osm_relation_id': 364776},
                {'name': 'Железнодорожный', 'type': 'district', 'osm_relation_id': 365341},
                {'name': 'Заельцовский', 'type': 'district', 'osm_relation_id': 365385},
                {'name': 'Калининский', 'type': 'district', 'osm_relation_id': 364762},
                {'name': 'Кировский', 'type': 'district', 'osm_relation_id': 365403},
                {'name': 'Ленинский', 'type': 'district', 'osm_relation_id': 365401},
                {'name': 'Октябрьский', 'type': 'district', 'osm_relation_id': 364764},
                {'name': 'Первомайский', 'type': 'district', 'osm_relation_id': 366541},
                {'name': 'Советский', 'type': 'district', 'osm_relation_id': 366519},
                {'name': 'Центральный', 'type': 'district', 'osm_relation_id': 364763},
            ],
        },
        "Екатеринбург": {
            "full_name": 'Екатеринбург',
            "osm_name": 'Екатеринбург',
            "center": [56.8389, 60.6057],
            "zoom": 12,
            "admin_level": '9',
            "zones_mode": "hybrid",
            "preset_zones": [
                {'name': 'Верх-Исетский', 'type': 'district', 'lat': 56.8387, 'lon': 60.5551, 'osm_relation_id': 5803327},
                {'name': 'Железнодорожный', 'type': 'district', 'lat': 56.8804, 'lon': 60.5549, 'osm_relation_id': 5818948},
                {'name': 'Кировский', 'type': 'district', 'lat': 56.8431, 'lon': 60.6737, 'osm_relation_id': 5818883},
                {'name': 'Ленинский', 'type': 'district', 'lat': 56.8266, 'lon': 60.5989, 'osm_relation_id': 5817698},
                {'name': 'Академический', 'type': 'district', 'lat': 56.7891, 'lon': 60.5235, 'geometry_queries': ['Академический район, Екатеринбург, Россия']},
                {'name': 'Октябрьский', 'type': 'district', 'lat': 56.8297, 'lon': 60.6688, 'osm_relation_id': 5803648},
                {'name': 'Орджоникидзевский', 'type': 'district', 'lat': 56.8960, 'lon': 60.6133, 'osm_relation_id': 5819002},
                {'name': 'Чкаловский', 'type': 'district', 'lat': 56.7607, 'lon': 60.6465, 'osm_relation_id': 5817295},
            ],
        },
        "Москва": {
            "full_name": 'Москва',
            "osm_name": 'Москва',
            "center": [55.7558, 37.6173],
            "zoom": 11,
            "admin_level": '9',
            "zones_mode": "hybrid",
            "preset_zones": [
                {'name': 'Центральный', 'type': 'district'},
                {'name': 'Северный', 'type': 'district'},
                {'name': 'Северо-Восточный', 'type': 'district'},
                {'name': 'Восточный', 'type': 'district'},
                {'name': 'Юго-Восточный', 'type': 'district'},
                {'name': 'Южный', 'type': 'district'},
                {'name': 'Юго-Западный', 'type': 'district'},
                {'name': 'Западный', 'type': 'district'},
                {'name': 'Северо-Западный', 'type': 'district'},
                {'name': 'Зеленоградский', 'type': 'district'},
                {'name': 'Новомосковский', 'type': 'district'},
                {'name': 'Троицкий', 'type': 'district'},
            ],
        },
        "Казань": {
            "full_name": 'Казань',
            "osm_name": 'Казань',
            "center": [55.7887, 49.1221],
            "zoom": 12,
            "admin_level": '9',
            "preset_zones": [
                {'name': 'Авиастроительный', 'type': 'district', 'osm_relation_id': 2133461},
                {'name': 'Вахитовский', 'type': 'district', 'osm_relation_id': 2133462},
                {'name': 'Кировский', 'type': 'district', 'osm_relation_id': 2133463},
                {'name': 'Московский', 'type': 'district', 'osm_relation_id': 2133464},
                {'name': 'Ново-Савиновский', 'type': 'district', 'osm_relation_id': 2133465},
                {'name': 'Приволжский', 'type': 'district', 'osm_relation_id': 2133466},
                {'name': 'Советский', 'type': 'district', 'osm_relation_id': 2133467},
            ],
        },
        "Нижний Новгород": {
            "full_name": 'Нижний Новгород',
            "osm_name": 'Нижний Новгород',
            "center": [56.2965, 43.9361],
            "zoom": 12,
            "admin_level": '9',
        },
        "Челябинск": {
            "full_name": 'Челябинск',
            "osm_name": 'Челябинск',
            "center": [55.1644, 61.4368],
            "zoom": 12,
            "admin_level": '9',
            "preset_zones": [
                {'name': 'Калининский', 'type': 'district', 'osm_relation_id': 1579611},
                {'name': 'Курчатовский', 'type': 'district', 'osm_relation_id': 1579610},
                {'name': 'Ленинский', 'type': 'district', 'osm_relation_id': 1581744},
                {'name': 'Металлургический', 'type': 'district', 'osm_relation_id': 1581688},
                {'name': 'Советский', 'type': 'district', 'osm_relation_id': 1581689},
                {'name': 'Тракторозаводский', 'type': 'district', 'osm_relation_id': 1581743},
                {'name': 'Центральный', 'type': 'district', 'osm_relation_id': 1579833},
            ],
        },
        "Самара": {
            "full_name": 'Самара',
            "osm_name": 'Самара',
            "center": [53.1959, 50.1002],
            "zoom": 12,
            "admin_level": '9',
            "preset_zones": [
                {'name': 'Железнодорожный', 'type': 'district', 'osm_relation_id': 283645},
                {'name': 'Кировский', 'type': 'district', 'osm_relation_id': 285953},
                {'name': 'Красноглинский', 'type': 'district', 'osm_relation_id': 285954},
                {'name': 'Куйбышевский', 'type': 'district', 'osm_relation_id': 283540},
                {'name': 'Ленинский', 'type': 'district', 'osm_relation_id': 283781},
                {'name': 'Октябрьский', 'type': 'district', 'osm_relation_id': 284542},
                {'name': 'Промышленный', 'type': 'district', 'osm_relation_id': 285136},
                {'name': 'Самарский', 'type': 'district', 'osm_relation_id': 283541},
                {'name': 'Советский', 'type': 'district', 'osm_relation_id': 284582},
            ],
        },
        "Омск": {
            "full_name": 'Омск',
            "osm_name": 'Омск',
            "center": [54.9885, 73.3242],
            "zoom": 12,
            "admin_level": '9',
        },
        "Ростов-на-Дону": {
            "full_name": 'Ростов-на-Дону',
            "osm_name": 'Ростов-на-Дону',
            "center": [47.2357, 39.7015],
            "zoom": 12,
            "admin_level": '9',
            "preset_zones": [
                {'name': 'Ворошиловский', 'type': 'district', 'osm_relation_id': 2228519},
                {'name': 'Железнодорожный', 'type': 'district', 'osm_relation_id': 2227607},
                {'name': 'Кировский', 'type': 'district', 'osm_relation_id': 2228364},
                {'name': 'Ленинский', 'type': 'district', 'osm_relation_id': 2227685},
                {'name': 'Октябрьский', 'type': 'district', 'osm_relation_id': 2228342},
                {'name': 'Первомайский', 'type': 'district', 'osm_relation_id': 2228520},
                {'name': 'Пролетарский', 'type': 'district', 'osm_relation_id': 2228370},
                {'name': 'Советский', 'type': 'district', 'osm_relation_id': 2227024},
            ],
        },
        "Уфа": {
            "full_name": 'Уфа',
            "osm_name": 'Уфа',
            "center": [54.7388, 55.9721],
            "zoom": 12,
            "admin_level": '9',
            "preset_zones": [
                {'name': 'Дёмский', 'type': 'district', 'osm_relation_id': 5523261},
                {'name': 'Калининский', 'type': 'district', 'osm_relation_id': 5523739},
                {'name': 'Кировский', 'type': 'district', 'osm_relation_id': 5523570},
                {'name': 'Ленинский', 'type': 'district', 'osm_relation_id': 5523346},
                {'name': 'Октябрьский', 'type': 'district', 'osm_relation_id': 5493970},
                {'name': 'Орджоникидзевский', 'type': 'district', 'osm_relation_id': 5523682},
                {'name': 'Советский', 'type': 'district', 'osm_relation_id': 3856973},
            ],
        },
        "Красноярск": {
            "full_name": 'Красноярск',
            "osm_name": 'Красноярск',
            "center": [56.0153, 92.8932],
            "zoom": 12,
            "admin_level": '9',
        },
        "Воронеж": {
            "full_name": 'Воронеж',
            "osm_name": 'Воронеж',
            "center": [51.672, 39.1843],
            "zoom": 12,
            "admin_level": '9',
        },
        "Пермь": {
            "full_name": 'Пермь',
            "osm_name": 'Пермь',
            "center": [58.0105, 56.2502],
            "zoom": 12,
            "admin_level": '9',
        },
        "Волгоград": {
            "full_name": 'Волгоград',
            "osm_name": 'Волгоград',
            "center": [48.708, 44.5133],
            "zoom": 12,
            "admin_level": '9',
        },
        "Краснодар": {
            "full_name": 'Краснодар',
            "osm_name": 'Краснодар',
            "center": [45.0355, 38.9753],
            "zoom": 12,
            "admin_level": '9',
        },
        "Тюмень": {
            "full_name": 'Тюмень',
            "osm_name": 'Тюмень',
            "center": [57.1522, 65.5272],
            "zoom": 12,
            "admin_level": '9',
        },
        "Саратов": {
            "full_name": 'Саратов',
            "osm_name": 'Саратов',
            "center": [51.5336, 46.0343],
            "zoom": 12,
            "admin_level": '9',
        },
        "Тольятти": {
            "full_name": 'Тольятти',
            "osm_name": 'Тольятти',
            "center": [53.5078, 49.4204],
            "zoom": 12,
            "admin_level": '9',
        },
        "Ижевск": {
            "full_name": 'Ижевск',
            "osm_name": 'Ижевск',
            "center": [56.8527, 53.2112],
            "zoom": 12,
            "admin_level": '9',
        },
        "Барнаул": {
            "full_name": 'Барнаул',
            "osm_name": 'Барнаул',
            "center": [53.3548, 83.7696],
            "zoom": 12,
            "admin_level": '9',
        },
        "Иркутск": {
            "full_name": 'Иркутск',
            "osm_name": 'Иркутск',
            "center": [52.2978, 104.2964],
            "zoom": 12,
            "admin_level": '9',
        },
        "Хабаровск": {
            "full_name": 'Хабаровск',
            "osm_name": 'Хабаровск',
            "center": [48.4827, 135.0838],
            "zoom": 12,
            "admin_level": '9',
        },
        "Ульяновск": {
            "full_name": 'Ульяновск',
            "osm_name": 'Ульяновск',
            "center": [54.3142, 48.4031],
            "zoom": 12,
            "admin_level": '9',
        },
        "Владивосток": {
            "full_name": 'Владивосток',
            "osm_name": 'Владивосток',
            "center": [43.1155, 131.8855],
            "zoom": 12,
            "admin_level": '9',
            "zones_mode": "hybrid",
            "max_zone_distance_km": 20,
            "preset_zones": [
                {'name': 'Ленинский', 'type': 'district', 'lat': 43.1236763, 'lon': 131.9834695, 'osm_relation_id': 1933824, 'geometry_queries': ['Ленинский район, Владивосток, Россия']},
                {'name': 'Первомайский', 'type': 'district', 'lat': 43.0825881, 'lon': 131.9473652, 'osm_relation_id': 1933826, 'subtract_relation_ids': [7063164, 2247428, 1933752], 'geometry_queries': ['Первомайский район, Владивосток, Россия']},
                {'name': 'Первореченский', 'type': 'district', 'lat': 43.1511528, 'lon': 131.9474794, 'osm_relation_id': 1933862, 'geometry_queries': ['Первореченский район, Владивосток, Россия']},
                {'name': 'Советский', 'type': 'district', 'lat': 43.2318736, 'lon': 132.0360693, 'osm_relation_id': 2517328, 'geometry_queries': ['Советский район, Владивосток, Россия']},
                {'name': 'Фрунзенский', 'type': 'district', 'lat': 43.1806376, 'lon': 131.7447793, 'osm_relation_id': 1933812, 'geometry_queries': ['Фрунзенский район, Владивосток, Россия']},
                {'name': 'Островные территории', 'type': 'district', 'lat': 42.995, 'lon': 131.815, 'geometry_queries': ['остров Русский, Владивосток, Россия', 'остров Попова, Владивосток, Россия', 'остров Рейнеке, Владивосток, Россия']},
            ],
        },
        "Ярославль": {
            "full_name": 'Ярославль',
            "osm_name": 'Ярославль',
            "center": [57.6261, 39.8845],
            "zoom": 12,
            "admin_level": '9',
        },
        "Махачкала": {
            "full_name": 'Махачкала',
            "osm_name": 'Махачкала',
            "center": [42.9849, 47.5047],
            "zoom": 12,
            "admin_level": '9',
        },
        "Томск": {
            "full_name": 'Томск',
            "osm_name": 'Томск',
            "center": [56.4846, 84.9482],
            "zoom": 12,
            "admin_level": '9',
        },
        "Оренбург": {
            "full_name": 'Оренбург',
            "osm_name": 'Оренбург',
            "center": [51.7727, 55.0988],
            "zoom": 12,
            "admin_level": '9',
        },
        "Кемерово": {
            "full_name": 'Кемерово',
            "osm_name": 'Кемерово',
            "center": [55.3616, 86.0884],
            "zoom": 12,
            "admin_level": '9',
        },
        "Рязань": {
            "full_name": 'Рязань',
            "osm_name": 'Рязань',
            "center": [54.6296, 39.7368],
            "zoom": 12,
            "admin_level": '9',
        },
        "Набережные Челны": {
            "full_name": 'Набережные Челны',
            "osm_name": 'Набережные Челны',
            "center": [55.7431, 52.3959],
            "zoom": 12,
            "admin_level": '9',
        },
        "Пенза": {
            "full_name": 'Пенза',
            "osm_name": 'Пенза',
            "center": [53.1959, 45.0183],
            "zoom": 12,
            "admin_level": '9',
        },
        "Липецк": {
            "full_name": 'Липецк',
            "osm_name": 'Липецк',
            "center": [52.6031, 39.5708],
            "zoom": 12,
            "admin_level": '9',
        },
        "Тула": {
            "full_name": 'Тула',
            "osm_name": 'Тула',
            "center": [54.1961, 37.6182],
            "zoom": 12,
            "admin_level": '9',
        },
        "Калининград": {
            "full_name": 'Калининград',
            "osm_name": 'Калининград',
            "center": [54.7104, 20.4522],
            "zoom": 12,
            "admin_level": '9',
        },
        "Сочи": {
            "full_name": 'Сочи',
            "osm_name": 'Сочи',
            "center": [43.5992, 39.7257],
            "zoom": 11,
            "admin_level": '8',
            "zones_mode": "hybrid",
            "max_zone_distance_km": 45,
            "preset_zones": [
                {'name': 'Центральный', 'type': 'district', 'lat': 43.5855, 'lon': 39.7231, 'osm_relation_id': 1116490, 'geometry_queries': ['Центральный внутригородской район, Сочи', 'Центральный район, Сочи, Россия']},
                {'name': 'Адлерский', 'type': 'district', 'lat': 43.4347, 'lon': 39.9333, 'osm_relation_id': 5650614, 'geometry_queries': ['Адлерский внутригородской район, Сочи', 'Адлер, Сочи']},
                {'name': 'Хостинский', 'type': 'district', 'lat': 43.5538, 'lon': 39.8447, 'osm_relation_id': 907728, 'subtract_relation_ids': [4454860], 'geometry_queries': ['Хостинский внутригородской район, Сочи', 'Хоста, Сочи']},
                {'name': 'Лазаревский', 'type': 'district', 'lat': 43.9089, 'lon': 39.3313, 'osm_relation_id': 1116460, 'extra_relation_ids': [4454860], 'geometry_queries': ['Лазаревский внутригородской район, Сочи', 'Лазаревское, Сочи']},
                {'name': 'Сириус', 'type': 'district', 'lat': 43.4067088, 'lon': 39.9654226, 'osm_relation_id': 11865377, 'geometry_queries': ['Сириус, Краснодарский край, Россия']},
            ],
        },
    },
}


_MOSCOW_AO_PRESETS = [
    {
        "name": "Центральный АО",
        "type": "district",
        "geometry_queries": ["Центральный административный округ, Москва, Россия"],
    },
    {
        "name": "Северный АО",
        "type": "district",
        "geometry_queries": ["Северный административный округ, Москва, Россия"],
    },
    {
        "name": "Северо-Восточный АО",
        "type": "district",
        "geometry_queries": ["Северо-Восточный административный округ, Москва, Россия"],
    },
    {
        "name": "Восточный АО",
        "type": "district",
        "geometry_queries": ["Восточный административный округ, Москва, Россия"],
    },
    {
        "name": "Юго-Восточный АО",
        "type": "district",
        "geometry_queries": ["Юго-Восточный административный округ, Москва, Россия"],
    },
    {
        "name": "Южный АО",
        "type": "district",
        "geometry_queries": ["Южный административный округ, Москва, Россия"],
    },
    {
        "name": "Юго-Западный АО",
        "type": "district",
        "geometry_queries": ["Юго-Западный административный округ, Москва, Россия"],
    },
    {
        "name": "Западный АО",
        "type": "district",
        "geometry_queries": ["Западный административный округ, Москва, Россия"],
    },
    {
        "name": "Северо-Западный АО",
        "type": "district",
        "geometry_queries": ["Северо-Западный административный округ, Москва, Россия"],
    },
    {
        "name": "Зеленоградский АО",
        "type": "district",
        "lat": 55.9825,
        "lon": 37.1815,
        "osm_relation_id": 1320358,
        "geometry_queries": [
            "Зеленоградский административный округ, Москва, Россия",
            "Зеленоград, Москва, Россия",
            "Зеленоградский административный округ города Москвы",
        ],
    },
    {
        "name": "Новомосковский АО",
        "type": "district",
        "geometry_queries": ["Новомосковский административный округ, Москва, Россия"],
    },
    {
        "name": "Троицкий АО",
        "type": "district",
        "lat": 55.4849,
        "lon": 37.3058,
        "geometry_queries": [
            "Троицкий административный округ, Москва, Россия",
            "Троицкий административный округ города Москвы",
            "Троицкий административный округ, Новая Москва, Россия",
            "ТАО, Москва, Россия",
        ],
    },
]


_HARDCODED_DISTRICT_PRESETS = {
    "Астрахань": ["Кировский", "Ленинский", "Советский", "Трусовский"],
    "Нижний Новгород": ["Автозаводский", "Канавинский", "Ленинский", "Московский", "Нижегородский", "Приокский", "Советский", "Сормовский"],
    "Омск": ["Кировский", "Ленинский", "Октябрьский", "Советский", "Центральный"],
    "Красноярск": ["Железнодорожный", "Кировский", "Ленинский", "Октябрьский", "Свердловский", "Советский", "Центральный"],
    "Воронеж": ["Железнодорожный", "Коминтерновский", "Левобережный", "Ленинский", "Советский", "Центральный"],
    "Пермь": ["Дзержинский", "Индустриальный", "Кировский", "Ленинский", "Мотовилихинский", "Орджоникидзевский", "Свердловский"],
    "Волгоград": ["Ворошиловский", "Дзержинский", "Кировский", "Красноармейский", "Краснооктябрьский", "Советский", "Тракторозаводский", "Центральный"],
    "Краснодар": ["Западный", "Карасунский", "Прикубанский", "Центральный"],
    "Тюмень": ["Восточный", "Калининский", "Ленинский", "Центральный"],
    "Саратов": ["Волжский", "Заводской", "Кировский", "Ленинский", "Октябрьский", "Фрунзенский"],
    "Тольятти": ["Автозаводский", "Комсомольский", "Центральный"],
    "Ижевск": ["Индустриальный", "Ленинский", "Октябрьский", "Первомайский", "Устиновский"],
    "Барнаул": ["Железнодорожный", "Индустриальный", "Ленинский", "Октябрьский", "Центральный"],
    "Иркутск": ["Кировский", "Куйбышевский", "Ленинский", "Октябрьский", "Правобережный", "Свердловский"],
    "Хабаровск": ["Железнодорожный", "Индустриальный", "Кировский", "Краснофлотский", "Центральный"],
    "Ярославль": ["Дзержинский", "Заволжский", "Кировский", "Красноперекопский", "Ленинский", "Фрунзенский"],
    "Махачкала": ["Кировский", "Ленинский", "Советский"],
    "Томск": ["Кировский", "Ленинский", "Октябрьский", "Советский"],
    "Оренбург": ["Дзержинский", "Ленинский", "Промышленный", "Центральный"],
    "Кемерово": ["Заводский", "Кировский", "Ленинский", "Рудничный", "Центральный"],
    "Рязань": ["Железнодорожный", "Московский", "Октябрьский", "Советский"],
    "Набережные Челны": ["Автозаводский", "Комсомольский", "Центральный"],
    "Пенза": ["Железнодорожный", "Ленинский", "Октябрьский", "Первомайский"],
    "Липецк": ["Левобережный", "Октябрьский", "Правобережный", "Советский"],
    "Тула": ["Зареченский", "Привокзальный", "Пролетарский", "Советский", "Центральный"],
    "Калининград": ["Ленинградский", "Московский", "Центральный"],
}


def _district_geometry_queries(city_name, district_name):
    city_name = str(city_name or "").strip()
    district_name = str(district_name or "").strip()
    queries = [
        f"{district_name} район, {city_name}, Россия",
        f"{district_name} район города {city_name}, Россия",
        f"{district_name} внутригородской район, {city_name}, Россия",
        f"{city_name}, {district_name} район",
        f"{city_name}, {district_name}",
    ]
    seen = set()
    deduped = []
    for query in queries:
        key = query.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(query)
    return deduped


def _district_name_presets(city_name, names):
    return [
        {
            "name": name,
            "type": "district",
            "geometry_queries": _district_geometry_queries(city_name, name),
        }
        for name in names
    ]


for _city_name, _city_info in CONFIG["cities"].items():
    if _city_name == "Москва":
        _city_info["preset_zones"] = _MOSCOW_AO_PRESETS
        _city_info["zone_label"] = "ao"
        _city_info["zones_mode"] = "preset"
        continue

    _city_info.setdefault("zone_label", "district")
    if not _city_info.get("preset_zones"):
        _city_info["preset_zones"] = _district_name_presets(
            _city_name,
            _HARDCODED_DISTRICT_PRESETS.get(_city_name, [_city_info["full_name"]])
        )
    _city_info["zones_mode"] = "preset"
