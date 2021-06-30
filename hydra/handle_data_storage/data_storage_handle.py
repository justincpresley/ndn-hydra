import asyncio as aio
from hashlib import blake2b
from typing import Dict
from ndn.app import NDNApp

from ndn.encoding.name import Name, Component
from ndn.types import InterestCanceled, InterestNack, InterestTimeout, ValidationFailure
from ..data_storage.data_storage import DataStorage


class DataStorageHandle:

    def __init__(self, app: NDNApp, config: Dict, data_storage: DataStorage) -> None:
        self.app = app
        self.config = config
        self.data_storage = data_storage

    # the main coroutine
    async def start(self):
        repo_prefix = Name.normalize(self.config['repo_prefix'])
        while True:
            fetchable_metainfos = self.data_storage.get_fetchable_metainfos()
            # fetchable_metainfos = []
            for fetchable_metainfo in fetchable_metainfos:
                fetch_path = Name.normalize(fetchable_metainfo['fetch_path'])
                fetch_path.append(Component.from_segment(fetchable_metainfo['packet']))
                try:
                    # print("started  fetching {0}".format(Name.to_str(fetch_path)))
                    data_name, meta_info, content = await self.app.express_interest(fetch_path, can_be_prefix = False)
                    # print("finished fetching {0}".format(Name.to_str(data_name)))
                    # print(type(content))
                    content = bytes(content)
                    digest = bytes(blake2b(content).digest()[:2])
                    # print("digest:")
                    # print(digest)
                    if digest == fetchable_metainfo['digest']:
                        print("fetched d: {}; origin d: {}; content: {}. correct".format(digest.hex(), fetchable_metainfo['digest'].hex(), content.decode()[:5]))
                        # print("correct digest {0}, digest corrects".format(Name.to_str(fetch_path)))
                        self.data_storage.update_metainfo_packet_fetched(fetchable_metainfo['insertion_id'], fetchable_metainfo['packet'])
                        self.data_storage.put_kv(fetchable_metainfo['key'], content)
                    else:
                        # print("incorrect digest {0}".format(Name.to_str(fetch_path)))
                        print("fetched d: {}; origin d: {}; content: {}".format(digest.hex(), fetchable_metainfo['digest'].hex(), content.decode()[:5]))
                except InterestNack as e:
                    print(f'Nacked with reason={e.reason}')
                except InterestTimeout:
                    print(f'Timeout')
                except InterestCanceled:
                    print(f'Canceled')
                except ValidationFailure:
                    print(f'Data failed to validate')

            # await aio.sleep(0.5)