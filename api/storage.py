import base64
import hashlib
import shutil
import sys
from pathlib import Path
from typing import List
from utils import get_parity

import schemas
from config import settings
from fastapi import Response, UploadFile, HTTPException, status
from loguru import logger
import aiofiles
import asyncio
import os


class Storage:
    def __init__(self, is_test: bool):
        logger.warning(f"NUM_DISKS: {settings.NUM_DISKS}")
        self.block_path: List[Path] = [
            Path("/tmp") / f"{settings.FOLDER_PREFIX}-{i}-test"
            if is_test
            else Path("/var/raid") / f"{settings.FOLDER_PREFIX}-{i}"
            for i in range(settings.NUM_DISKS)
        ]
        self.__create_block()

    def __create_block(self):
        for path in self.block_path:
            logger.warning(f"Creating folder: {path}")
            path.mkdir(parents=True, exist_ok=True)

    def read_block(self, block_file):
        with open(block_file, 'rb') as f:
            return f.read()

    def write_block(self, block_file, content):
        if not block_file.parent.exists():
            block_file.parent.mkdir(parents=True, exist_ok=True)
        with open(block_file, 'wb') as f:
            f.write(content)

    async def file_integrity(self, filename: str) -> bool:
        """TODO: check if file integrity is valid
        file integrated must satisfy following conditions:
            1. all data blocks must exist
            2. size of all data blocks must be equal
            3. parity block must exist
            4. parity verify must success

        if one of the above conditions is not satisfied
        the file does not exist
        and the file is considered to be damaged
        so we need to delete the file
        """

        for block_path in self.block_path:
            block_file = Path.joinpath(block_path, filename)
            if not block_file.exists():
                logger.error(f"Block file not exist: {block_file}")
                await self._delete_file(filename)
                return False

        for i in range(1, len(self.block_path)):
            prev_block_file = self.read_block(Path.joinpath(self.block_path[i - 1], filename))
            block_file = self.read_block(Path.joinpath(self.block_path[i], filename))
            if len(prev_block_file) != len(block_file):
                logger.error(f"Block size not equal: {self.block_path[i]} {self.block_path[i - 1]}")
                await self._delete_file(filename)
                return False

        blocks = []
        for i in range(settings.NUM_DISKS - 1):
            blocks.append(self.read_block(self.block_path[i] / filename))

        _parity = get_parity(blocks)
        parity = self.read_block(Path.joinpath(self.block_path[-1], filename))
        print(_parity, parity)
        if _parity != parity:
            logger.error(f"Parity verify failed: {_parity} {parity}")
            await self._delete_file(filename)
            return False

        return True

    async def create_file(self, file: UploadFile) -> schemas.File:
        import math
        content = await file.read()
        checksum = hashlib.md5(content).hexdigest()

        if await self.file_integrity(file.filename):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail="File already exists", headers={'content-type': 'application/json'}
            )

        # calculate block size
        L = len(content)
        if L > settings.MAX_SIZE:
            logger.error(f"File too large: {file.filename}, size: {L}")
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="File too large", headers={"content-type": "application/json"}
            )
        logger.info("creating file: {file.filename} with size: {L}")
        N = settings.NUM_DISKS - 1
        block_size = math.floor(L / N)
        blocks = []
        cur_offset = 0
        for i in range(N):
            if i < L % N:
                blocks.append(content[cur_offset:cur_offset + block_size + 1])
                cur_offset += block_size + 1
            else:
                blocks.append(content[cur_offset:cur_offset + block_size])
                cur_offset += block_size

        # make sure all blocks are equally sized
        for i in range(len(blocks)):
            if len(blocks[i]) < block_size + 1:
                blocks[i] += b'\x00' * (block_size + 1 - len(blocks[i]))

        # write to each storage device
        for block, block_path in zip(blocks, self.block_path):
            self.write_block(Path.joinpath(block_path, file.filename), block)

        # compute and write the parity block
        parity = get_parity(blocks)

        self.write_block(Path.joinpath(self.block_path[-1], file.filename), bytes(parity))

        return schemas.File(
            name=file.filename,
            size=len(content),
            checksum=checksum,
            content=base64.b64encode(content),
            content_type=file.content_type,
        )

    async def retrieve_file(self, filename: str) -> bytes:
        # check if file exists
        if not await self.file_integrity(filename):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="File not found", headers={"content-type": "application/json"}
            )

        blocks = [self.read_block(block_path / filename) for block_path in self.block_path[:-1]]
        content = bytearray()
        for i, block in enumerate(blocks):
            content += block.strip(b'\x00')
        return bytes(content)

    async def _delete_file(self, filename: str) -> None:
        for block_path in self.block_path:
            if (block_path / filename).exists():
                os.remove(block_path / filename)
            elif not block_path.exists():
                (block_path).mkdir(parents=True, exist_ok=True)

    async def delete_file(self, filename: str) -> None:
        if not await self.file_integrity(filename):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="File not found", headers={"content-type": "application/json"}
            )

        await self._delete_file(filename)

    async def update_file(self, file) -> schemas.File:
        # check if file exists
        if not await self.file_integrity(file.filename):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="File not found", headers={"content-type": "application/json"}
            )

        await self._delete_file(file.filename)
        return await self.create_file(file)

    async def fix_block(self, block_id: int) -> None:
        # obtain all filenames in the affected block
        filename = set()
        healthy_block_path = []
        for block_path in self.block_path:
            if not block_path.exists():
                continue
            healthy_block_path.append(block_path)
            filenames = [f.name for f in (block_path).glob('*')]
            filename.update(filenames)
        filenames = list(filename)

        for filename in filenames:
            # read all other blocks and compute the XOR parity
            blocks = [self.read_block(block_path / filename)
                      for i, block_path in enumerate(self.block_path) if i != block_id and (block_path / filename).exists()]

            # if there's no block to compute parity from, continue with the next file
            if not blocks:
                continue
            parity = get_parity(blocks)

            # write the computed parity to the missing block
            self.block_path[block_id].mkdir(parents=True, exist_ok=True)
            self.write_block(self.block_path[block_id] / filename, bytes(parity))


storage: Storage = Storage(is_test="pytest" in sys.modules)
