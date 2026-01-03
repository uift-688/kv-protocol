from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Any, Optional, Type, TypedDict, Literal, Union, Mapping, TypeAlias, TypeGuard, overload, cast
from re import compile
from .db import DatabaseBase
from struct import unpack, pack
from cbor2 import loads, dumps
from base64 import b64decode, b64encode


class PointerInfoType(TypedDict):
    target: str

POINTERPATTERN = compile(r"Pointer\?(.+)")
NATIVE_PATTERN = compile(r"Native\?(.+)")
T = TypeVar("T")
class Object(ABC, Generic[T]):
    TYPE: Type[Any]
    name: str
    db: DatabaseBase
    @abstractmethod
    async def execute(self) -> T:
        ...
    @classmethod
    @abstractmethod
    async def from_data(cls, name: str, data: T, db: DatabaseBase) -> "Object[T]": ...
    @classmethod
    @abstractmethod
    def tit(cls, info: str) -> bool: ...
    @classmethod
    @abstractmethod
    async def from_name(cls, name: str, db: DatabaseBase) -> "Object[T]": ...

    @overload
    @classmethod
    def select(cls, data: str) -> Optional[Type["Object[Any]"]]: ...
    @overload
    @classmethod
    def select(cls, data: Any) -> Optional[Type["Object[Any]"]]: ...

    @classmethod
    def select(cls, data: Any) -> Optional[Type["Object[Any]"]]:
        if isinstance(data, str):
            subcls: Optional[Type[Object[Any]]] = None
            for subcls in cls.__subclasses__():
                tit = subcls.tit(data)
                if tit:
                    break
            return subcls
        else:
            subcls: Optional[Type[Object[Any]]] = None
            for subcls in cls.__subclasses__():
                if isinstance(data, subcls.TYPE):
                    break
            return subcls

    @abstractmethod
    async def destroy(self): ...

NativeTypeType: TypeAlias = Literal["Integer", "Decimal", "String", "Binary", "Boolean"]
class NativeTypeInfo(TypedDict):
    type: NativeTypeType
    data: bytes

def native_type_loads(data: str) -> Union[bytes, int, float, str, bool]:
    match = NATIVE_PATTERN.fullmatch(data)
    if not match:
        raise ValueError("This is not Native Type.")
    info: NativeTypeInfo = loads(b64decode(match.group(1)))
    ntype = info["type"]
    ndata = info["data"]
    if ntype == "Binary":
        return ndata
    elif ntype == "Boolean":
        return unpack("!?", ndata)[0]
    elif ntype == "Decimal":
        return unpack("d", ndata)[0]
    elif ntype == "Integer":
        return unpack("q", ndata)[0]
    elif ntype == "String":
        return ndata.decode()
    raise TypeError("Unknown type.")

NATIVE_TYPES: TypeAlias = Union[bytes, int, float, str, bool]
_NATIVE_TYPES_TUPLE = (bytes, int, float, str, bool)
_NATIVE_TYPE_MAPPING: Mapping[type, NativeTypeType] = {bytes: "Binary", int: "Integer", float: "Decimal", bool: "Boolean", str: "String"}
def native_type_dumps(data: NATIVE_TYPES) -> str:
    ntype = _NATIVE_TYPE_MAPPING.get(type(data))
    ndata = None
    if isinstance(data, bytes):
        ndata = data
    elif isinstance(data, bool):
        ndata = pack("!?", data)
    elif isinstance(data, float):
        ndata = pack("d", data)
    elif isinstance(data, int):
        ndata = pack("q", data)
    elif isinstance(data, str):
        ndata = data.encode()
    if ntype is None or ndata is None:
        raise TypeError("Unknown type.")
    info = NativeTypeInfo(type=ntype, data=ndata)
    return "Native?" + b64encode(dumps(info)).decode()

def is_native_info(data: str):
    return NATIVE_PATTERN.fullmatch(data) is not None

def is_native_type(data: Any) -> TypeGuard[NATIVE_TYPES]:
    return isinstance(data, _NATIVE_TYPES_TUPLE)

async def from_pointer(info: str, db: DatabaseBase) -> Union[Object[Any], NATIVE_TYPES]:
    match = POINTERPATTERN.fullmatch(info)
    if match:
        name = match.group(1)
        value = await db.get(name)
        fdata = None
        if is_native_info(value):
            fdata = native_type_loads(value)
        else:
            otype = Object.select(value)
            if otype is None:
                raise TypeError("Unknown type.")
            fdata = await otype.from_name(name, db)
        return fdata
    else:
        raise ValueError("This is not a pointer.")


def to_pointer(data: Object[Any]):
    return f"Pointer?{data.name}"

class DatabaseWrapper(Generic[T]):
    def __init__(self, db: DatabaseBase) -> None:
        self.db = db
    async def set(self, key: str, value: Any):
        if isinstance(value, Object):
            pointer = to_pointer(value)
            await self.db.set(key, pointer)
        else:
            if is_native_type(value):
                info = native_type_dumps(value)
                await self.db.set(key, info)
            else:
                otype = Object.select(value)
                if otype is None:
                    raise TypeError("Unknown Error.")
                await otype.from_data(key, value, self.db)
    async def get(self, key: str, default: Optional[Union[T, Object[T]]] = None) -> Union[Object[T], T, None]:
        if not await self.db.exists(key):
            return default
        info = await self.db.get(key)
        if is_native_info(info):
            return cast(Union[T, Object[T]], native_type_loads(info))
        otype = Object.select(info)
        if otype is None:
            raise TypeError("Unknown type.")
        return await otype.from_name(key, self.db)
    async def delete(self, key: str):
        info = await self.db.get(key)
        if is_native_info(info):
            await self.db.delete(key)
        else:
            otype = Object.select(info)
            if otype is None:
                raise TypeError("Unknown type.")
            obj = await otype.from_name(key, self.db)
            await obj.destroy()
    def exists(self, key: str):
        return self.db.exists(key)
