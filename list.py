from typing import Generic, TypeVar, Self, MutableSequence, Coroutine, Any, Type, Final, Union, Optional, cast, TypeGuard
from .object import Object, POINTERPATTERN, native_type_loads, is_native_type, native_type_dumps, from_pointer, to_pointer, is_native_info
from .db import DatabaseBase
from types import TracebackType
from cbor2 import dumps
from base64 import b64encode, b64decode
from struct import pack, unpack


T = TypeVar("T")
class List(Object[MutableSequence[T]], Generic[T]):
    TYPE = list
    META: Final[str] = b64encode(dumps({"type": "<List 1>"})).decode()
    def __init__(self, db: DatabaseBase, name: str) -> None:
        self.db = db
        self.name = name
        self.queue: MutableSequence[Coroutine[Any, Any, None]] = []
    async def __aenter__(self) -> Self:
        return self
    async def __aexit__(self, ext: Type[Exception], exv: Exception, tb: TracebackType):
        queues = self.queue[:]
        self.queue.clear()
        for queue in queues:
            await queue
    @classmethod
    def tit(cls, info: str) -> TypeGuard[Self]:
        return cls.META == info
    @classmethod
    async def from_data(cls, name: str, data: MutableSequence[Union[Object[T], T]], db: DatabaseBase) -> Self:
        instance = cls(db, name)
        length_data = b64encode(pack("H", len(data))).decode()
        await db.set(f"{name}", cls.META)
        await db.set(f"{name}_length", length_data)
        async with instance as dc:
            for i, val in enumerate(data):
                dc[i] = val
        await db.set(f"{name}_length", length_data)
        return instance
    @classmethod
    async def from_name(cls, name: str, db: DatabaseBase) -> "List[T]":
        return cls(db, name)
    async def __getitem__(self, key: int) -> Union[Object[T], T]:
        is_exist = await self.db.exists(f"{self.name}_i_{key}")
        if not is_exist:
            raise IndexError(key)
        value = await self.db.get(f"{self.name}_i_{key}")
        is_pointer = POINTERPATTERN.fullmatch(value)
        if is_pointer:
            fdata = await from_pointer(value, self.db)
            return cast(T, fdata)
        if is_native_info(value):
            return cast(T, native_type_loads(value))
        else:
            otype = Object.select(value)
            if otype is None:
                raise TypeError("Unknown type.")
            return await otype.from_name(f"{self.name}_i_{key}")
    def __setitem__(self, key: int, value: Union[T, Object[T]]):
        async def wrapper():
            if await self.length() <= key:
                raise IndexError(key)
            is_pointer = isinstance(value, Object)
            if is_native_type(value) and not is_pointer:
                await self.db.set(f"{self.name}_i_{key}", native_type_dumps(value))
            else:
                obj: Object[T]
                if is_pointer:
                    obj = cast(Object[T], value)
                else:
                    objt = Object.select(value)
                    if objt is None:
                        raise TypeError("Unknown type.")
                    obj = await objt.from_data(f"{key}_from_{self.name}", value, self.db)
                await self.db.set(f"{self.name}_i_{key}", to_pointer(obj))
            now_length: int = unpack("H", b64decode(await self.db.get(f"{self.name}_length")))[0]
            await self.db.set(f"{self.name}_length", b64encode(pack("H", now_length + 1)).decode())
        self.queue.append(wrapper())
    async def execute(self):
        data: MutableSequence[Union[T, Object[T]]] = []
        async for i in self:
            data.append(i)
        return data
    async def length(self) -> int:
        return unpack("H", b64decode(await self.db.get(f"{self.name}_length")))[0]
    async def __aiter__(self):
        for i in range(await self.length()):
            yield await self[i]
    def __delitem__(self, index: int):
        async def wrapper():
            length = await self.length()
            values = await self.execute()
            await self.db.delete(f"{self.name}_i_{index}")
            async with self:
                for i in range(index + 1, length):
                    value = values[i]
                    self[i - 1] = value
            await self.db.delete(f"{self.name}_i_{length - 1}")
            await self.db.set(f"{self.name}_length", b64encode(pack("H", length - 1)).decode())
        self.queue.append(wrapper())
    async def append(self, data: T):
        append_target = await self.length()
        await self.db.set(f"{self.name}_length", b64encode(pack("H", append_target + 1)).decode())
        async with self:
            self[append_target] = data
    async def clear(self):
        for i in range(await self.length()):
            del self[i]
    async def destroy(self):
        await self.clear()
        await self.db.delete(self.name)
        await self.db.delete(f"{self.name}_length")

