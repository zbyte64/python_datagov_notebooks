import aiohttp, asyncio, pandas, os, requests, collections, urllib, shutil
from time import sleep
import json

host_semaphores = collections.defaultdict(asyncio.Semaphore) #only 1 request per host name
infinity = float('inf')

#TODO log fn
async def peek_if_csv_and_write(dataset, max_readline=1024*12, max_size=infinity, allow_no_size=True, parent_folder='datasets'):
    if not os.path.exists(parent_folder):
        os.mkdir(parent_folder)
    url = dataset['res_url'][dataset['res_format'].index('CSV')]
    folder = os.path.join(parent_folder, dataset['id'])
    complete_flag = os.path.join(folder, 'complete')
    if os.path.exists(complete_flag):
        #print(folder, "Already exists")
        return True
    elif not os.path.exists(folder):
        os.mkdir(folder)
    json.dump(dataset, open(os.path.join(folder, '_metadata.json'), 'w'), indent='\t')
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
                    return False

                print(folder, "writing complete flag")
                open(complete_flag, 'w').close()
                return dst
        except asyncio.TimeoutError:
            print(folder, "timed out")
            shutil.rmtree(folder, True)
            return False
        except (aiohttp.errors.ServerDisconnectedError, ConnectionResetError) as error:
            print(folder, "angry server", error)
            shutil.rmtree(folder, True)
            return False
        finally:
            host_semaphores[host].release()


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

#download all the csvs
#TODO turn into coroutine
i = 0
while True:
    offset = i * 10
    raw_response = requests.get('http://catalog.data.gov/api/search/dataset', params={'all_fields':1, 'res_format':'CSV', 'offset': offset})
    try:
        response = raw_response.json()
    except:
        print("ERROR", ras_response.status)
        sleep(5)
        continue
    results = response['results']
    futures = map(peek_if_csv_and_write, results)
    loop.run_until_complete(asyncio.gather(*futures))

    if response['count'] < offset + 10:
        break

loop.close()
