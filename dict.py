from typing import MutableMapping, Generic, TypeVar, Self, MutableSequence, Coroutine, Any, Type, Final, Union, cast, TypeGuard, Optional
from .object import Object, POINTERPATTERN, native_type_loads, is_native_type, native_type_dumps, from_pointer, to_pointer, is_native_info
from .db import DatabaseBase
from types import TracebackType
from cbor2 import dumps, loads
from base64 import b64encode, b64decode


T = TypeVar("T")
class Dictionary(Object[MutableMapping[str, T]], Generic[T]):
    TYPE = dict
    META: Final[str] = b64encode(dumps({"type": "<Dictionary 1>"})).decode()
    def __init__(self, db: DatabaseBase, name: str) -> None:
        self.db = db
        self.name = name
        self.queue: MutableSequence[Coroutine[Any, Any, None]] = []
    async def __aenter__(self) -> Self:
        return self
    async def __aexit__(self, ext: Type[Exception], exv: Exception, tb: TracebackType):
        for queue in self.queue[:]:
            await queue
        self.queue.clear()
    @classmethod
    def tit(cls, info: str) -> TypeGuard[Self]:
        return cls.META == info
    @classmethod
    async def from_data(cls, name: str, data: MutableMapping[str, Union[Object[T], T]], db: DatabaseBase) -> Self:
        instance = cls(db, name)
        await db.set(f"{name}", cls.META)
        await db.set(f"{name}_keys", b64encode(dumps(list(data.keys()))).decode())
        async with instance as dc:
            for key, val in data.items():
                dc[key] = val
        return instance
    async def keys(self) -> MutableSequence[str]:
        keys = await self.db.get(f"{self.name}_keys")
        keys_decoded = loads(b64decode(keys))
        return keys_decoded
    async def values(self):
        for key in await self.keys():
            yield await self[key]
    @classmethod
    async def from_name(cls, name: str, db: DatabaseBase) -> "Dictionary[T]":
        return cls(db, name)
    async def __getitem__(self, key: str) -> Union[Object[T], T]:
        is_exist = await self.db.exists(f"{self.name}_k_{key}")
        if not is_exist:
            raise KeyError(key)
        value = await self.db.get(f"{self.name}_k_{key}")
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
            return await otype.from_name(f"{self.name}_k_{key}")
    def __setitem__(self, key: str, value: Union[T, Object[T]]):
        async def wrapper():
            is_pointer = isinstance(value, Object)
            if is_native_type(value) and not is_pointer:
                await self.db.set(f"{self.name}_k_{key}", native_type_dumps(value))
            else:
                obj: Object[T]
                if is_pointer:
                    obj = cast(Object[T], value)
                else:
                    objt = Object.select(value)
                    if objt is None:
                        raise TypeError("Unknown type.")
                    obj = await objt.from_data(f"{key}_from_{self.name}", value, self.db)
                await self.db.set(f"{self.name}_k_{key}", to_pointer(obj))
            await  self.db.set(f"{self.name}_keys", b64encode(dumps(set(await self.keys()) | {key})).decode())
        self.queue.append(wrapper())
    async def execute(self):
        data: MutableMapping[str, Union[T, Object[T]]] = {}
        for key in await self.keys():
            data[key] = await self[key]
        return data
    async def __aiter__(self):
        for key in await self.keys():
            yield key
    async def items(self):
        for key in await self.keys():
            yield (key, await self[key])
    async def get(self, key: str, default: Optional[T] = None):
        try:
            return await self[key]
        except KeyError:
            return default
    async def has(self, key: str):
        return await self.db.exists(f"{self.name}_k_{key}")
    def __delitem__(self, key: str):
        async def wrapper():
            await self.db.delete(f"{self.name}_k_{key}")
            await self.db.set(f"{self.name}_keys", b64encode(dumps(list(set(await self.keys()) - {key}))).decode())
        self.queue.append(wrapper())
    async def destroy(self):
        for key in await self.keys():
            del self[key]
        await self.db.delete(self.name)
        await self.db.delete(f"{self.name}_keys")
