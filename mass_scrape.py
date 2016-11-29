import aiohttp, asyncio, pandas, os, requests, collections, urllib, shutil
from concurrent.futures import TimeoutError
from time import sleep
import json

host_semaphores = collections.defaultdict(asyncio.Semaphore) #only 1 request per host name
infinity = float('inf')

#TODO log fn
async def peek_if_csv_and_write(dataset, max_readline=1024*12, max_size=infinity, allow_no_size=True, parent_folder='datasets'):
    if not os.path.exists(parent_folder):
        os.mkdir(parent_folder)

    folder = os.path.join(parent_folder, dataset['id'])
    if not os.path.exists(folder):
        os.mkdir(folder)

    complete_flag = os.path.join(folder, 'complete')
    if os.path.exists(complete_flag):
        #print(folder, "Already exists")
        return True

    json.dump(dataset, open(os.path.join(folder, '_metadata.json'), 'w'), indent='\t')

    url = dataset['res_url'][dataset['res_format'].index('CSV')]
    for index, res_description in enumerate(dataset['res_description']):
        if '.original.' in res_description:
            print(folder, 'skipping derrived dataset')
            return False
            url = dataset['res_url'][index]


    host = urllib.parse.urlparse(url).netloc
    await host_semaphores[host].acquire()
    print(folder, "Downloading:", url)
    async with aiohttp.ClientSession() as session:
        try:
            resp_with_timer = await asyncio.wait_for(session.get(url), 5.0)
            async with resp_with_timer as resp:
                if resp.status != 200:
                    print(folder, "got non 200 response:", resp.status)
                    return False
                #content-disposition: attachment; filename=foo.pdf
                #content-length
                if 'CONTENT-DISPOSITION' not in resp.headers:
                    print(folder, "no content disposition found")
                    print(folder, "writing complete flag")
                    open(complete_flag, 'w').close()
                    return False
                dst = resp.headers['CONTENT-DISPOSITION'].split('; filename=')[-1]
                dst = os.path.join(folder, dst)
                print(folder, "Destination:", dst)
                size = int(resp.headers.get('CONTENT-LENGTH', 0))
                if size is 0 and not allow_no_size:
                    print(folder, "no size reported")
                    return False
                if size > max_size:
                    print(folder, "too big of a file")
                    return False
                if dst.endswith('.csv'):
                    read_bytes = await resp.content.read(max_readline)
                    if b'\n' not in read_bytes:
                        print(folder, "no new line found, probably not a csv")
                        return False
                    output = open(dst, 'wb')
                    output.write(read_bytes)
                    while True:
                        chunk = await resp.content.read()
                        if chunk:
                            output.write(chunk)
                        else:
                            break
                    output.close()
                    try:
                        pandas.read_csv(dst, nrows=3)
                    except Exception as error:
                        print(folder, "Not a CSV:", error)
                        return False
                elif dst.endswith('.zip'):
                    if size is 0 and not allow_no_size:
                        #ouch, dangerous
                        print(folder, "zipfile with no size reported, skipping")
                        return False
                    #TODO figure out folder structure to handle this
                    bytes_so_far = 0
                    output = open(dst, 'wb')
                    while True:
                        if bytes_so_far > max_size:
                            return False #TOO big of a zip file and they didn't tell us how big!
                        chunk = await resp.content.read()
                        if chunk:
                            bytes_so_far += len(chunk)
                            output.write(chunk)
                        else:
                            break
                    output.close()
                    #TODO unzip
                else:
                    print(folder, "Unrecognized filetype:", dst)
                    bytes_so_far = 0
                    output = open(dst, 'wb')
                    while True:
                        if bytes_so_far > max_size:
                            return False #TOO big of a file and they didn't tell us how big!
                        chunk = await resp.content.read()
                        if chunk:
                            bytes_so_far += len(chunk)
                            output.write(chunk)
                        else:
                            break
                    output.close()

                print(folder, "writing complete flag")
                open(complete_flag, 'w').close()
                return dst
        except (asyncio.TimeoutError, TimeoutError):
            print(folder, "timed out")
            return False
        except (aiohttp.errors.ServerDisconnectedError, ConnectionResetError) as error:
            print(folder, "angry server", error)
            return False
        finally:
            host_semaphores[host].release()


async def que_datasets(url, params):
    async with aiohttp.ClientSession() as session:
        resp_with_timer = await session.get(url, params=params) #asyncio.wait_for(session.get(url, params=params), 5.0)
        async with resp_with_timer as resp:
            if resp.status != 200:
                print("got non 200 response:", resp.status)
                return [], 0
            json_response = await resp.json()
            results = json_response['results']
            futures = map(peek_if_csv_and_write, results)
            return futures, json_response['count']


async def download_data_gov():
    i = 0
    while True:
        print('Downloading page:', i)
        offset = i * 10
        futures, count = await que_datasets(
            'http://catalog.data.gov/api/search/dataset',
            {'all_fields':1, 'res_format':'CSV', 'offset': offset}
        )

        await asyncio.gather(*futures, asyncio.sleep(5))

        i += 1

        if count < offset + 10:
            break


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(download_data_gov())
loop.close()
