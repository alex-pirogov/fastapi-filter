import json
from typing import Any, Generic, List, Sequence, Tuple, Type, TypeVar, cast

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import Select, desc, func, select, text
from sqlalchemy.sql import functions

from app.database.base_model import BaseModel as ORMBaseModel
from app.utils.base_schema import BaseSchema

from .schema import BaseFilterSchema, FilterFieldInfo, LookupInfo

_OBM = TypeVar('_OBM', bound=ORMBaseModel)
_BS = TypeVar('_BS', bound=BaseSchema)

ORM_LOOKUP_MAPPING = {
    'neq': '__ne__',
    'gt': '__gt__',
    'gte': '__ge__',
    'lt': '__lt__',
    'lte': '__le__',
    'in': 'in_',
    'not_in': 'not_in',
    'regexp': 'regexp_match'
}


class BasePaginator(BaseModel, Generic[_BS]):
    page: int = Field(default=0, ge=0)
    per_page: int = Field(default=10, ge=1, le=100)
    results: Sequence[_BS]

_BP = TypeVar('_BP', bound=BasePaginator[Any])

class BaseFilter(Generic[_OBM, _BS, _BP]):
    orm_model: Type[_OBM]
    schema: Type[BaseFilterSchema]
    ordering_param: str = 'order_by'
    search_param: str = 'search'
    default_ordering: str = 'id'
    paginator_class: Type[_BP]

    def __init_subclass__(cls) -> None:
        for field in ('orm_model', 'schema'):
            if field not in cls.__dict__:
                raise ValueError(f"'{field}' class attribute is required")
            
        return super().__init_subclass__()

    def __init__(
            self,
            request: Request
        ) -> None:
        self.request = request
        
        self._query = select(self.orm_model)
        
        try:
            self.paginator = self.paginator_class.model_validate(
                {**self.request.query_params, 'results': []}
            )

        except ValidationError as e:
            raise self.get_exception(*json.loads(e.json(include_url=False)))
    
    @staticmethod
    def get_exception(*errors: str):
        return RequestValidationError(errors) 

    def inject_query(self, query: Select[Tuple[_OBM]]):
        self._query = query
        return self

    def filter(self):
        lookups: List[LookupInfo] = []

        for key, value in self.request.query_params.items():
            if key in (
                    self.ordering_param,
                    self.search_param,
                    'page',
                    'per_page',
            ):
                continue

            try:
                field_name, query_lookup = key.split('__')
                try:
                    lookup = ORM_LOOKUP_MAPPING[query_lookup]
                except KeyError:
                    raise self.get_exception(f"Unknown lookup '{query_lookup}'")

            except ValueError:
                field_name, lookup = key, '__eq__'
            
            if field_name not in self.schema.model_fields:
                raise self.get_exception(f"Unknown filtering field '{field_name}'")

            lookups.append(LookupInfo(field_name, lookup, value))
    
        try:
            validated_filter = self.schema.model_validate(
                obj={
                    lookup.field_name: lookup.value
                    for lookup
                    in lookups
                },
                context={
                    lookup.field_name: lookup.lookup
                    for lookup
                    in lookups
                }
            )

        except ValidationError as e:
            raise self.get_exception(*json.loads(e.json(include_url=False)))
        
        for lookup in lookups:
            model_field = getattr(self.orm_model, lookup.field_name)
            self._query = self._query.filter(
                getattr(model_field, lookup.lookup)(getattr(validated_filter, lookup.field_name))
            )
        
        return self
    
    def search(self):
        seqrch_query = self.request.query_params.get(self.search_param)
        if not seqrch_query:
            return self

        search_fields = [
            key for key, value
            in self.schema.model_fields.items()
            if cast(FilterFieldInfo, value).searchable
        ]

        pk_field = getattr(self.orm_model, 'id')
        sq = select(
            pk_field,
            func.similarity(
                functions.concat(
                    *[
                        getattr(self.orm_model, field_name)
                        for field_name
                        in search_fields
                    ]
                ),
                seqrch_query
            ).label('result')
        ).subquery('a')
    
        self._query = (
            self._query
            .join(sq, pk_field == text('a.id'))
            .order_by(desc(text('result')))
        )

        return self

    def order(self):
        order_by = self.request.query_params.get(self.ordering_param)
        if not order_by:
            order_by = self.default_ordering
        
        if order_by[0] == '-':
            order, field_name = 'desc', order_by[1:]
        else:
            order = 'asc'

            if order_by[0] == '+':
                field_name = order_by[1:]
            else:
                field_name = order_by
        
        if field_name not in self.schema.model_fields:
            raise self.get_exception(f"Unknown ordering field '{field_name}'")
        
        order_field = cast(FilterFieldInfo, self.schema.model_fields[field_name])
        if not order_field.orderable:
            raise self.get_exception(f"Ordering by '{field_name}' is not permitted")

        model_field = getattr(self.orm_model, field_name)
        self._query = self._query.order_by(getattr(model_field, order)())

        return self
    
    def offset(self):
        self._query = self._query.offset((self.paginator.page)*self.paginator.per_page)
        return self

    def limit(self):
        self._query = self._query.limit(self.paginator.per_page)
        return self

    def full(self):
        return self.filter().search().order().offset().limit()

    def get_query(self) -> Select[Tuple[_OBM]]:
        return self._query

    def get_response(self, scalars: Sequence[_BS]) -> _BP:
        return self.paginator_class(
            page=self.paginator.page,
            per_page=self.paginator.per_page,
            results=scalars
        )
