import base64
import hashlib
import sys
from pathlib import Path
from typing import List

import schemas
from config import settings
from fastapi import UploadFile
from loguru import logger
import aiofiles
import asyncio
import os


class Storage:
    def __init__(self, is_test: bool):
        logger.warning(f"NUM_DISKS: {settings.NUM_DISKS}")
        self.block_path: List[Path] = [
            Path(settings.UPLOAD_PATH) / f"{settings.FOLDER_PREFIX}-{i}"
            if is_test
            else Path("/var/raid") / f"{settings.FOLDER_PREFIX}-{i}"
            for i in range(settings.NUM_DISKS)
        ]
        self.__create_block()

    def __create_block(self):
        for path in self.block_path:
            logger.warning(f"Creating folder: {path}")
            path.mkdir(parents=True, exist_ok=True)

    # TODO: this code is generated by gpt, please check it
    async def file_integrity(self, filename: str) -> bool:
        block_files = [block_path / filename for block_path in self.block_path]

        # 1. all data blocks must exist
        if not all(os.path.isfile(block_file) for block_file in block_files):
            return False

        # 2. size of all data blocks must be equal
        sizes = [os.path.getsize(block_file) for block_file in block_files]
        if len(set(sizes)) != 1:
            return False

        # 3. parity block must exist
        parity_path = self.block_path[-1] / filename
        if not os.path.isfile(parity_path):
            return False

        # 4. parity verify must success
        # read all blocks and compute the XOR parity
        blocks = [await self.read_block(block_file) for block_file in block_files]
        parity = await self.read_block(parity_path)
        computed_parity = bytearray(blocks[0])
        for i in range(1, len(blocks)):
            for j in range(len(computed_parity)):
                computed_parity[j] ^= blocks[i][j]
        if computed_parity != parity:
            return False

        return True

    # TODO: this code is generated by gpt, please check it
    async def read_block(self, block_file):
        async with aiofiles.open(block_file, 'rb') as f:
            return await f.read()

    # TODO: this code is generated by gpt, please check it
    async def write_block(self, block_file, content):
        async with aiofiles.open(block_file, 'wb') as f:
            await f.write(content)

    async def create_file(self, file: UploadFile) -> schemas.File:
        import math
        content = await file.read()
        checksum = hashlib.md5(content).hexdigest()

        # calculate block size
        L = len(content)
        N = settings.NUM_DISKS - 1
        block_size = math.floor(L/N)
        last_block_size = math.floor(L/N)
        blocks = []
        cur_offset = 0
        for i in range(N):
            if i < L % N:
                blocks.append(content[cur_offset:cur_offset+block_size+1])
                cur_offset += block_size + 1
            else:
                blocks.append(content[cur_offset:cur_offset+block_size])
                cur_offset += block_size

        print(blocks)
        print([len(block) for block in blocks])

        # make sure all blocks are equally sized
        for i in range(len(blocks)):
            if len(blocks[i]) < block_size + 1:
                blocks[i] += b'\x00' * (block_size+1 - len(blocks[i]))

        # write to each storage device
        for block, block_path in zip(blocks, self.block_path):
            await self.write_block(Path.joinpath(block_path, file.filename), block)

        # compute and write the parity block
        parity = bytearray(blocks[0])
        for i in range(1, len(blocks)):
            for j in range(len(parity)):
                parity[j] ^= blocks[i][j]

        
        await self.write_block(Path.joinpath(self.block_path[-1], file.filename), bytes(parity))

        return schemas.File(
            name=file.filename,
            size=len(content),
            checksum=checksum,
            content=base64.b64encode(content),
            content_type=file.content_type,
        )

    # TODO: this code is generated by gpt, please check it
    async def retrieve_file(self, filename: str) -> bytes:
        blocks = [await self.read_block(block_path / filename) for block_path in self.block_path[:-1]]
        content = bytearray(sum(len(block) for block in blocks))
        for i, block in enumerate(blocks):
            content[i::settings.NUM_DISKS - 1] = block
        return bytes(content)

    # TODO: this code is generated by gpt, please check it
    async def delete_file(self, filename: str) -> None:
        for block_path in self.block_path:
            try:
                os.remove(block_path / filename)
            except FileNotFoundError:
                pass

    # TODO: this code is generated by gpt, please check it
    async def fix_block(self, block_id: int) -> None:
        # obtain all filenames in the affected block
        filenames = [f.name for f in (self.block_path[block_id]).glob('*')]

        for filename in filenames:
            # read all other blocks and compute the XOR parity
            blocks = [await self.read_block(block_path / filename)
                      for i, block_path in enumerate(self.block_path) if i != block_id and (block_path / filename).exists()]

            # if there's no block to compute parity from, continue with the next file
            if not blocks:
                continue

            parity = bytearray(blocks[0])
            for i in range(1, len(blocks)):
                for j in range(len(parity)):
                    parity[j] ^= blocks[i][j]

            # write the computed parity to the missing block
            await self.write_block(self.block_path[block_id] / filename, bytes(parity))


storage: Storage = Storage(is_test="pytest" in sys.modules)
