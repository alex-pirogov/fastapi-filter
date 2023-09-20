from typing import Any, List, NamedTuple, cast

import typing_extensions
from pydantic import (BaseModel, FieldValidationInfo, ValidationError,
                      create_model, field_validator)
from pydantic.fields import FieldInfo
from pydantic.fields import \
    _FromFieldInfoInputs as _BaseFromFieldInfoInputs  # type: ignore
from pydantic_core import InitErrorDetails, PydanticUndefined
from typing_extensions import Unpack

# TODO
# what if field is repeated?


class LookupInfo(NamedTuple):
    field_name: str
    lookup: str
    value: Any


class BaseFilterSchema(BaseModel):
    @field_validator('*', mode='plain')
    @classmethod
    def name_must_contain_space(cls, v: str, info: FieldValidationInfo) -> Any:
        InitialTypeAnnotation = cls.model_fields[info.field_name].annotation
        
        if InitialTypeAnnotation is None:
            raise ValueError(f"Unknown field type")

        if not info.context:
            raise ValueError(f"Context with lookups is required")
        
        lookup = info.context.get(info.field_name)

        if lookup in ('in_', 'not_in'):
            TypeAnnotation = cast(List[Any], List[InitialTypeAnnotation])
            value_to_validate = v.split(',')

        else:
            TypeAnnotation = InitialTypeAnnotation
            value_to_validate = v

        ValidationModel = create_model(
            'ValidationModel',
            __config__=None,
            __base__=None,
            __module__=__name__,
            __validators__=None,
            __cls_kwargs__=None,
            **{
                info.field_name: (TypeAnnotation, ...)
            }
        )

        try:
            validated = ValidationModel.model_validate(
                {info.field_name: value_to_validate}
            )
        except ValidationError as e:
            errors: List[InitErrorDetails] = []
            for error in e.errors(include_url=False):
                error['loc'] = error['loc'][1:]
                errors.append(InitErrorDetails(error)) # type: ignore

            raise ValidationError.from_exception_data(
                e.title,
                line_errors=errors
            )
        
        return getattr(validated, info.field_name)


class _FromFieldInfoInputs(_BaseFromFieldInfoInputs, total=False):
    orderable: bool
    searchable: bool


class FilterFieldInfo(FieldInfo):
    __slots__ = (
        'orderable',
        'searchable'
    )

    def __init__(self, **kwargs: Unpack[_FromFieldInfoInputs]) -> None:
        self.orderable = kwargs.pop('orderable', True)
        self.searchable = kwargs.pop('searchable', True)
        super().__init__(**kwargs)

    @classmethod
    def from_field(
        cls, default: Any = PydanticUndefined, **kwargs: Unpack[_FromFieldInfoInputs]
    ) -> typing_extensions.Self:
        return super().from_field(default=default, **kwargs)
    

def FilterField(  # noqa: C901
        orderable: bool = True,
        searchable: bool = True
) -> Any:
    return FilterFieldInfo.from_field(
        default=None,
        orderable=orderable,
        searchable=searchable
    )
