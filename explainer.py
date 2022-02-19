import argparse
from os import PathLike
import re
from hashlib import sha256
from abc import ABC, abstractmethod, abstractproperty
from typing import BinaryIO, Iterable, Union
from matplotlib import pyplot as plt
from matplotlib.artist import Artist
from matplotlib.image import AxesImage
import boto3
from botocore.exceptions import ClientError
from io import BytesIO
import zounds
import html
import os.path
from datetime import datetime
from time import time
from copy import deepcopy
from pprint import pprint

class CodeResultRenderer(ABC):

    @abstractproperty
    def content_type(self):
        pass

    @abstractmethod
    def matches(self, result: any) -> bool:
        pass

    @abstractmethod
    def render(self, result: any) -> BinaryIO:
        pass

    @abstractmethod
    def html(self, url: str) -> str:
        pass


class RendererLocator(object):
    def __init__(self, *renderers: Iterable[CodeResultRenderer]):
        super().__init__()
        self._renderers = renderers

    def find_renderer(self, result: any) -> Union[CodeResultRenderer, None]:
        try:
            return next(filter(lambda x: x.matches(result), self._renderers))
        except StopIteration:
            return None


class CodeBlock(object):

    def __init__(self, source: str):
        super().__init__()
        self._raw = source

    @property
    def markdown(self):
        return f'\n```python\n{self._raw}```\n'

    @property
    def raw(self):
        return self._raw

    @property
    def normalized(self):
        return '\n'.join(filter(lambda x: bool(x), self._raw.splitlines()))

    def get_result(self, glob: dict):
        # print('===========================================================')
        # print(self.normalized)
        # start = time()
        bytecode = compile(self.normalized, 'placeholder.dat', mode='exec')
        exec(bytecode, glob)
        # print(f'Compilation and execution took {time() - start:.2f} seconds')
        return glob.get('_', None)


class EmbeddedCodeBlock(object):

    pattern = re.compile(r'```python:\s+(?P<code>[^`]+)```')

    def __init__(self, block: CodeBlock, start: int, span: int, position: int):
        super().__init__()
        self._block = block
        self._start = start
        self._span = span
        self._position = position

    def get_result(self, glob: dict):
        result = self._block.get_result(glob)
        return glob, result

    @property
    def markdown(self):
        return self._block.markdown

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._start + self._span

    @property
    def raw(self):
        return self._block.raw

    def content_key(self, preceeding=''):
        h = sha256(f'position: {self._position}'.encode())
        h.update(preceeding.encode())
        h.update(self._block.normalized.encode())
        return h.hexdigest()

    @classmethod
    def extract_all(cls, markdown: str):
        for i, m in enumerate(cls.pattern.finditer(markdown)):
            yield EmbeddedCodeBlock(
                CodeBlock(m.groupdict()['code']),
                start=m.start(),
                span=m.end() - m.start(),
                position=i)


class S3Client(object):

    def __init__(self, bucket_name: str):
        super().__init__()
        self._bucket_name = bucket_name
        self._client = boto3.client('s3')
        self._create_bucket()

    def _create_bucket(self):
        try:
            self._client.create_bucket(
                ACL='public-read',
                Bucket=self._bucket_name)
            print(f'Creating bucket {self._bucket_name}')
        except self._client.exceptions.BucketAlreadyExists:
            pass

    def key_exists(self, key: str) -> bool:
        try:
            self._client.head_object(
                Bucket=self._bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def store_key(self, key: str, data: BinaryIO, content_type: str):
        if not self.key_exists(key):
            print(f'Storing key {key} with content type {content_type}')
            self._client.put_object(
                Bucket=self._bucket_name,
                Key=key, Body=data,
                ACL='public-read',
                ContentType=content_type)
        else:
            print(f'key {key} already stored')

        return f'https://{self._bucket_name}.s3.amazonaws.com/{key}'


class PlotRenderer(CodeResultRenderer):
    def __init__(self, client: S3Client):
        super().__init__()
        self._client = client

    @property
    def content_type(self):
        return 'image/png'

    def matches(self, result: any):
        try:
            return isinstance(result, AxesImage) \
                or isinstance(result[0], Artist)
        except (IndexError, TypeError):
            return False

    def render(self, result: any):
        bio = BytesIO()
        plt.savefig(bio, format='png')
        plt.clf()
        bio.seek(0)
        return bio

    def html(self, url: str) -> str:
        return f'<img src="{html.escape(url)}" />'


class AudioRenderer(CodeResultRenderer):
    def __init__(self, client: S3Client):
        super().__init__()
        self._client = client

    @property
    def content_type(self):
        return 'audio/ogg'

    def matches(self, result: any):
        return isinstance(result, zounds.AudioSamples)

    def render(self, result: zounds.AudioSamples):
        return result.encode(fmt='OGG', subtype='vorbis')

    def html(self, url: str) -> str:
        return f'<audio controls src="{html.escape(url)}"></audio>'



def render_html(
        markdown_path: PathLike,
        output_path: PathLike,
        s3: S3Client,
        render_locator: RendererLocator,
        result_cache) -> dict:

    with open(markdown_path, 'r') as f:
        content = f.read()

        code_blocks = list(EmbeddedCodeBlock.extract_all(content))

        if len(code_blocks) == 0:
            with open(output_path, 'w') as output_file:
                output_file.write(content)
                return

        g = {}
        current_pos = 0
        preceeding = ''

        keys_of_interest = set(['a'])
        print('============================================\n\n\n')

        chunks = []
        for block in code_blocks:
            print('--------------------------------------------')
            chunks.append(content[current_pos:block.start])
            chunks.append(block.markdown)
            current_pos = block.end + 1

            preceeding = content_key = block.content_key(preceeding)
            try:
                g, result = result_cache[content_key]
                print(f'Pulled {content_key} from cache')
                print('with locals')
                print({k: g.get(k, None) for k in keys_of_interest})
            except KeyError:
                print(f'Computing {content_key}')
                print('with locals')
                print({k: g.get(k, None) for k in keys_of_interest})
                
                g, result = block.get_result(dict(**g))

                # shallow copy of the state
                result_cache[content_key] = dict(**g), result
            
            renderer: CodeResultRenderer = render_locator.find_renderer(result)

            if renderer is not None:
                print(f'Rendering {content_key}')
                bio = renderer.render(result)
                url = s3.store_key(
                    content_key, 
                    bio,
                    renderer.content_type)
                html = renderer.html(url)
                chunks.append(html)
            elif result is not None:
                print(f'Rendering code block')
                chunks.append(f'`{result}`')
            
            try:
                del g['_']
            except KeyError:
                pass

        chunks.append(content[current_pos:])

        markdown = '\n'.join(chunks)
        with open(output_path, 'w') as output_file:
            output_file.write(markdown)
        
        return result_cache


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--markdown',
        required=True,
        type=str,
        help='Path to the mardown file used to generate output')
    parser.add_argument(
        '--output',
        required=True,
        type=str,
        help='Path where the output html should be saved')
    parser.add_argument(
        '--s3',
        required=True,
        type=str,
        help='The s3 bucket where code block results should be stored')
    parser.add_argument(
        '--watch',
        default=False,
        action='store_true',
        help='Watch the markdown file for changes and regenerate html when detected')

    args = parser.parse_args()

    client = S3Client(args.s3)
    render_locator = RendererLocator(
        PlotRenderer(client),
        AudioRenderer(client)
    )

    if os.path.isfile(args.output):
        output_path = args.output
    else:
        md_filename = os.path.basename(args.markdown)
        output_path = os.path.join(args.output, md_filename)

    result_cache = {}

    render_html(
        args.markdown, output_path, client, render_locator, result_cache)

    if args.watch:
        import inotify.adapters
        notify = inotify.adapters.Inotify()
        notify.add_watch(args.markdown)
        for event in notify.event_gen(yield_nones=False):
            (_, type_names, path, filename) = event
            if 'IN_CLOSE_WRITE' in type_names:
                frame_cache = render_html(
                    args.markdown, output_path, client, render_locator, result_cache)        