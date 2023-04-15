import mimetypes
import os
import re
from functools import lru_cache
from typing import Iterable, Union, Any, Tuple, Mapping

from gchar.resources.pixiv import get_pixiv_keywords
from hbutils.system import urlsplit
from hbutils.testing import disable_output
from hfmirror.resource import SyncResource, RemoteSyncItem
from hfmirror.resource.item import register_sync_type
from hfmirror.utils import TargetPathType, srequest, get_requests_session
from imgutils.tagging import get_wd14_tags
from pixivpy3 import AppPixivAPI
from tqdm.auto import tqdm

REMOTE_PIXIV_SESSION_INDEX_URL = 'REMOTE_PIXIV_SESSION_INDEX_URL'


@lru_cache()
def _get_remote_session_index_raw():
    remote_index_url = os.environ.get(REMOTE_PIXIV_SESSION_INDEX_URL, None)
    assert remote_index_url, f'{REMOTE_PIXIV_SESSION_INDEX_URL!r} not given in environment.'

    session = get_requests_session()
    resp = srequest(session, 'GET', remote_index_url)
    remote_urls = resp.json()

    resp = srequest(session, 'GET', remote_urls[0])
    return resp.json()


@lru_cache()
def _get_refresh_token():
    return _get_remote_session_index_raw()["refresh_token"]


pixiv = AppPixivAPI()
pixiv.auth(refresh_token=_get_refresh_token())


class PixivRemoteItem(RemoteSyncItem):
    __type__ = 'pixiv'
    __request_kwargs__ = {'headers': {"Referer": 'https://app-api.pixiv.net/'}}

    def get_new_session(self):
        return pixiv.requests


register_sync_type(PixivRemoteItem)


class PixivFewShotResource(SyncResource):
    def __init__(self, characters, counts, use_original: bool = True, threshold: float = 0.5,
                 cross_check: bool = True, max_official: int = 3):
        SyncResource.__init__(self)
        self.characters = characters
        self.counts = counts
        self.use_original = use_original
        self.threshold = threshold
        self.cross_check = cross_check
        self.max_official = max_official
        self.session = get_requests_session()

    def grab(self) -> Iterable[Union[
        Tuple[str, Any, TargetPathType, Mapping],
        Tuple[str, Any, TargetPathType],
    ]]:
        max_cnt = max(self.counts)
        ch_tqdm = tqdm(self.characters, leave=False)
        for ch in ch_tqdm:
            ch_tqdm.set_description(f'{ch.index} - {ch.cnname}')
            keyword = get_pixiv_keywords(ch, exclude=['R-18'])
            offset = 0
            items = []

            c_tqdm = tqdm(desc=f'{ch.index} - {ch.cnname} - c{max_cnt}', total=max_cnt, leave=False)

            for skin in ch.skins[:self.max_official]:
                _, ext = os.path.splitext(urlsplit(skin.url).filename)
                if not ext:
                    resp = srequest(self.session, 'HEAD', skin.url)
                    ext = mimetypes.guess_extension(resp.headers['Content-Type'])

                filename = re.sub(r'\W+', '_', skin.name).strip('_') + ext
                items.append(('remote', skin.url, filename, {'official': True, 'name': skin.name}))
                c_tqdm.update()

            while True:
                api_response = pixiv.search_illust(keyword, sort='popular_desc', offset=offset)
                illusts = api_response['illusts']
                offset += len(illusts)
                if not illusts:
                    break

                for image in illusts:
                    if image['illust_ai_type'] == 2:
                        continue
                    if image['type'] != 'illust':
                        continue

                    id_ = image["id"]
                    page_count = image["page_count"]
                    if page_count == 1:
                        original_image_urls = [image["meta_single_page"]["original_image_url"]]
                        large_image_urls = [image["image_urls"]["large"]]
                    else:
                        original_image_urls = [x["image_urls"]["original"] for x in image["meta_pages"]]
                        large_image_urls = [x["image_urls"]["large"] for x in image["meta_pages"]]

                    image_urls = original_image_urls if self.use_original else large_image_urls
                    if len(image_urls) == 1:
                        url = image_urls[0]
                        large_url = large_image_urls[0]

                        with disable_output(), PixivRemoteItem(large_url, {}, []).load_file() as f:
                            rating, tags, chars = get_wd14_tags(f)
                            if tags.get('solo', 0.0) < self.threshold:
                                continue
                            if self.cross_check:
                                if (ch.gender == 'female' and tags.get('1girl', 0.0) < self.threshold) or \
                                        (ch.gender == 'male' and tags.get('1boy', 0.0) < self.threshold):
                                    continue

                        items.append(('pixiv', url, urlsplit(url).filename, {'pixivid': id_, 'chars': chars}))
                        c_tqdm.update()

                    if len(items) >= max_cnt:
                        break

                if len(items) >= max_cnt:
                    break

            for current_cnt in self.counts:
                for op, url, filename, metadata in items[:current_cnt]:
                    yield op, url, f'{ch.index}/{current_cnt}/{filename}', metadata

                yield 'metadata', {
                    'expected': current_cnt,
                    'actual': len(items[:current_cnt]),
                }, f'{ch.index}/{current_cnt}'

            yield 'metadata', {
                'game': type(ch).__game_name__,
                'id': ch.index,
                'cnname': str(ch.cnname) if ch.cnname else None,
                'jpname': str(ch.jpname) if ch.jpname else None,
                'enname': str(ch.enname) if ch.enname else None,
                'alias': list(map(str, ch.alias_names)),
            }, f'{ch.index}'
