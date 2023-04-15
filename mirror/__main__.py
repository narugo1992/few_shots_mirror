import os
from functools import partial

import click
from gchar.games.dispatch.access import CHARS
from gchar.resources.pixiv import query_pixiv_illustration_count_by_character
from hfmirror.storage import HuggingfaceStorage
from hfmirror.sync import SyncTask
from huggingface_hub import HfApi

from .resource import PixivFewShotResource
from .utils import GLOBAL_CONTEXT_SETTINGS, print_version


@click.group(context_settings={**GLOBAL_CONTEXT_SETTINGS}, help='Utils for sync data.')
@click.option('-v', '--version', is_flag=True,
              callback=partial(print_version, 'mirror'), expose_value=False, is_eager=True,
              help="Show version information.")
def cli():
    pass  # pragma: no cover


@cli.command('sync', help="Transport files to huggingface",
             context_settings={**GLOBAL_CONTEXT_SETTINGS})
@click.option('--game', '-g', 'game', type=click.Choice(list(map(lambda x: x.__game_name__, CHARS))),
              required=True, help='Game for sync.')
@click.option('--min_images', 'min_images', type=int, default=500,
              help='Min Images', show_default=True)
@click.option('--repo', '-r', 'repo', type=str, default='deepghs/few_shots',
              help='Repository to upload.', show_default=True)
@click.option('--namespace', '-n', 'namespace', type=str, default=None,
              help="Namespace to upload. Resource will be used when not given.", show_default=True)
def sync(game, repo, namespace, min_images):
    namespace = game if namespace is None else namespace
    all_chs = {c.__game_name__: c for c in CHARS}[game].all(contains_extra=False)
    all_chs = [ch for ch in all_chs if query_pixiv_illustration_count_by_character(ch)[0] >= min_images]
    all_chs = sorted(all_chs, key=lambda x: query_pixiv_illustration_count_by_character(x)[0])
    sync = PixivFewShotResource(all_chs, [3, 5, 10])

    api = HfApi(token=os.environ.get('HF_TOKEN'))
    api.create_repo(repo, repo_type='dataset', exist_ok=True)
    storage = HuggingfaceStorage(repo, hf_client=api, namespace=namespace)

    task = SyncTask(sync, storage)
    task.sync()


if __name__ == '__main__':
    cli()
