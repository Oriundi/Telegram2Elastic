from elasticsearch import Elasticsearch

from telegram2elastic import OutputWriter
import logging

LOG_LEVEL_INFO = 35

class Writer(OutputWriter):
    def __init__(self, config: dict):
        super().__init__(config)

        self.index_format = config.get("index_format", "messages")

        username = config.get("username")
        password = config.get("password")

        if username is not None and password is not None:
            http_auth = [username, password]
        else:
            http_auth = None

        self.client = Elasticsearch(hosts=config.get("host", "localhost"), basic_auth=http_auth,
                                    verify_certs=False, ssl_show_warn=False)
        # self.client = Elasticsearch(hosts=config.get("host", "localhost"))

        resp = self.client.info()
        logging.log(LOG_LEVEL_INFO, resp)

    async def write_message(self, message):
        doc_data = await self.get_message_dict(message)

        doc_data["timestamp"] = message.date

        # get_message_dict() by default adds "id" and "date" which should not be in the body
        if "id" in doc_data:
            del doc_data["id"]
        if "date" in doc_data:
            del doc_data["date"]

        print(f'{message.id} :: {doc_data}')
        logging.log(LOG_LEVEL_INFO, f'{message.id} :: {doc_data}')

        # self.client.index(index=message.date.strftime(self.index_format), body=doc_data, id=message.id)
        self.client.index(index=self.index_format, body=doc_data, id=message.id)

    def create_index(self, index_name='messages'):
        created = False
        # index settings
        # settings = json.loads('elastic_mapping.json')

        settings = {
            "settings": {
                "index": {
                    "codec": "default",
                    "refresh_interval": "2s",
                    "shard.check_on_startup": "checksum",
                    "mapping.coerce": "false",
                    "max_result_window": 10000,
                    "number_of_shards": 4,
                    "number_of_replicas": 0,
                    "routing_partition_size": 1,
                    "sort.field": "timestamp",
                    "sort.order": "desc",
                    "queries": {
                        "cache": {
                            "enabled": "true"
                        }
                    }
                },
                "analysis": {
                    "analyzer": {
                        "lowercase": {
                            "type": "custom",
                            "filter": [
                                "lowercase",
                                "word_delimiter",
                                "ru_stop",
                                "ru_stemmer"
                            ],
                            "tokenizer": "standard"
                        },
                        "default": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "wd",
                                "ru_stop",
                                "ru_stemmer"
                            ]
                        }
                    },
                    "filter": {
                        "wd": {
                            "type": "word_delimiter",
                            "catenate_all": True,
                            "preserve_original": False
                        },
                        "asciifolding": {
                            "type": "asciifolding",
                            "preserve_original": True
                        },
                        "ru_stop": {
                            "type": "stop",
                            "stopwords": "_russian_"
                        },
                        "ru_stemmer": {
                            "type": "stemmer",
                            "language": "russian"
                        }
                    },
                    "normalizer": {
                        "default": {
                            "type": "custom",
                            "char_filter": [],
                            "filter": [
                                "lowercase",
                                "asciifolding"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "_routing": {
                    "required": False
                },
                "_source": {
                    "enabled": True,
                    "excludes": []
                },
                "dynamic": False,
                "properties": {
                    "chat": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "chat_id": {
                        "type": "long"
                    },
                    "user_id": {
                        "type": "long"
                    },
                    "username": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "firstName": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "lastName": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "phone" : {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "strict_date_optional_time"
                    },
                    "message_id": {
                        "type": "long"
                    },
                    "message": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            }
        }

        try:
            if not self.client.indices.exists(index=index_name):
                # Ignore 400 means to ignore "Index Already Exist" error.
                self.client.indices.create(index=index_name, ignore=400, body=settings)
                print('Created Index')
            created = True
        except Exception as ex:
            print(str(ex))
        finally:
            return created
