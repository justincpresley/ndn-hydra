# -------------------------------------------------------------
# NDN Hydra Concurrent Segment Fetcher
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------
# NOTE: This concurrent_fetcher was originally written by
#       jonnykong@cs.ucla.edu on 2019-10-15 and later modified
#       to meet the demands of Hydra.
# -------------------------------------------------------------

import asyncio as aio
import logging
from ndn.app import NDNApp
from ndn.types import InterestNack, InterestTimeout
from ndn.encoding import Name, NonStrictName, Component
from typing import Optional

#An async-generator to fetch data packets concurrently.
async def concurrent_fetcher(app: NDNApp, name: NonStrictName, file_name: NonStrictName, start_block_id: int,
                             end_block_id: Optional[int], semaphore: aio.Semaphore, **kwargs):
    cur_id = start_block_id
    final_id = end_block_id if end_block_id is not None else 0x7fffffff
    is_failed = False
    tasks = []
    recv_window = cur_id - 1
    seq_to_data_packet = dict() # Buffer for out-of-order delivery
    received_or_fail = aio.Event()

    async def _retry(seq: int):
        """
        Retry 3 times fetching data of the given sequence number or fail.
        :param seq: block_id of data
        """
        nonlocal app, name, file_name, semaphore, is_failed, received_or_fail, final_id
        int_name = Name.normalize(name) + [Component.from_segment(seq)]
        # print(Name.to_str(int_name))
        key = Name.normalize(file_name) + [Component.from_segment(seq)]
        # print(Name.to_str(key))

        trial_times = 0
        while True:
            trial_times += 1
            if trial_times > 3:
                semaphore.release()
                is_failed = True
                received_or_fail.set()
                return
            try:
                #logging.info('Express Interest: {}'.format(Name.to_str(int_name)))
                data_name, meta_info, content, data_bytes = await app.express_interest(
                    int_name, need_raw_packet=True, can_be_prefix=True, must_be_fresh=False, lifetime=4000, **kwargs)

                # Save data and update final_id
                #logging.info('Received data: {}'.format(Name.to_str(data_name)))
                seq_to_data_packet[seq] = (data_name, meta_info, content, data_bytes, key)
                if meta_info is not None and meta_info.final_block_id is not None:
                    final_id = Component.to_number(meta_info.final_block_id)
                break
            except InterestNack as e:
                logging.info(f'Nacked with reason={e.reason} {Name.to_str(int_name)}')
            except InterestTimeout:
                logging.info(f'Timeout {Name.to_str(int_name)}')
        semaphore.release()
        received_or_fail.set()

    async def _dispatch_tasks():
        """
        Dispatch retry() tasks using semaphore.
        """
        nonlocal semaphore, tasks, cur_id, final_id, is_failed
        while cur_id <= final_id:
            await semaphore.acquire()
            if is_failed:
                received_or_fail.set()
                semaphore.release()
                break
            task = aio.get_event_loop().create_task(_retry(cur_id))
            tasks.append(task)
            cur_id += 1

    aio.get_event_loop().create_task(_dispatch_tasks())
    while True:
        await received_or_fail.wait()
        received_or_fail.clear()
        # Re-assemble bytes in order
        while recv_window + 1 in seq_to_data_packet:
            yield seq_to_data_packet[recv_window + 1]
            del seq_to_data_packet[recv_window + 1]
            recv_window += 1
        # Return if all data have been fetched, or the fetching process failed
        if recv_window == final_id:
            await aio.gather(*tasks)
            return
        if is_failed:
            await aio.gather(*tasks)
            # New data may return during gather(), need to check again
            while recv_window + 1 in seq_to_data_packet:
                yield seq_to_data_packet[recv_window + 1]
                del seq_to_data_packet[recv_window + 1]
                recv_window += 1
            return